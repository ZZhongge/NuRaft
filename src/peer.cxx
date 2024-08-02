/************************************************************************
Modifications Copyright 2017-2019 eBay Inc.
Author/Developer(s): Jung-Sang Ahn

Original Copyright:
See URL: https://github.com/datatechnology/cornerstone

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "peer.hxx"

#include "debugging_options.hxx"
#include "tracer.hxx"

#include <unordered_set>

namespace nuraft {

void peer::send_req( ptr<peer> myself,
                     ptr<req_msg>& req,
                     rpc_handler& handler )
{
    if (abandoned_) {
        p_er("peer %d has been shut down, cannot send request",
             config_->get_id());
        return;
    }

    if (req) {
        p_tr("send req %d -> %d, type %s",
             req->get_src(),
             req->get_dst(),
             msg_type_to_string( req->get_type() ).c_str() );
    }

    ptr<rpc_result> pending = cs_new<rpc_result>(handler);
    ptr<rpc_client> rpc_local = nullptr;
    {   std::lock_guard<std::mutex> l(rpc_protector_);
        if (!rpc_) {
            // Nothing will be sent, immediately free it
            // to serve next operation.
            p_tr("rpc local is null");
            set_free();
            return;
        }
        rpc_local = rpc_;
    }
    rpc_handler h = (rpc_handler)std::bind
                    ( &peer::handle_rpc_result,
                      this,
                      myself,
                      rpc_local,
                      req,
                      pending,
                      std::placeholders::_1,
                      std::placeholders::_2 );
    if (rpc_local) {
        rpc_local->send(req, h);
    }
}

void peer::send_req_with_write_callback(ptr<peer> myself,
                ptr<req_msg>& req,
                rpc_handler& handler, rpc_handler& write_handler)
{
    if (abandoned_) {
        p_er("peer %d has been shut down, cannot send request",
             config_->get_id());
        return;
    }

    if (req) {
        p_tr("send req %d -> %d, type %s",
             req->get_src(),
             req->get_dst(),
             msg_type_to_string( req->get_type() ).c_str() );
    }

    ptr<rpc_result> pending = cs_new<rpc_result>(handler);
    ptr<rpc_client> rpc_local = nullptr;
    {   std::lock_guard<std::mutex> l(rpc_protector_);
        if (!rpc_) {
            // Nothing will be sent, immediately free it
            // to serve next operation.
            p_tr("rpc local is null");
            write_done();
            set_free();
            return;
        }
        rpc_local = rpc_;
    }
    rpc_handler h = (rpc_handler)std::bind
                    ( &peer::handle_rpc_result,
                      this,
                      myself,
                      rpc_local,
                      req,
                      pending,
                      std::placeholders::_1,
                      std::placeholders::_2 );
    rpc_handler w = (rpc_handler)std::bind
                ( &peer::handle_write_done,
                    this,
                    myself,
                    rpc_local,
                    req,
                    h,
                    write_handler,
                    std::placeholders::_1,
                    std::placeholders::_2 );
    if (rpc_local) {
        rpc_local->send_using_write_callback(req, w);
    }
}

void peer::handle_write_done( ptr<peer> myself,
                              ptr<rpc_client> my_rpc_client,
                              ptr<req_msg>& req,
                              rpc_handler& when_done,
                              rpc_handler& handle_write_done,
                              ptr<resp_msg>& resp,
                              ptr<rpc_exception>& err )
{
    // only for append_entries_request
    if (err) {
        when_done(resp, err);
        myself->write_done();
        return;
    }

    {
        std::lock_guard<std::mutex> l(rpc_protector_);
        // check rpc_id
        uint64_t cur_rpc_id = rpc_ ? rpc_->get_id() : 0;
        uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
        if (cur_rpc_id != given_rpc_id) {
            p_wn( "[EDGE CASE] got stale RPC write down callback from %d: "
                    "current %p (%" PRIu64 "), from parameter %p (%" PRIu64 "). "
                    "will ignore this write down callback",
                    config_->get_id(),
                    rpc_.get(),
                    cur_rpc_id,
                    my_rpc_client.get(),
                    given_rpc_id );
            return;
        }

        
        auto_lock(pending_read_reqs_lock_);
        // process pending request
        if (pending_read_reqs_.empty()) {
            p_db("no pending reqs, start to read, start_log_idx: %ld", req->get_last_log_idx());
            my_rpc_client->async_read_response(req, when_done);
        }
        pending_read_reqs_.push_back(cs_new<PeerReqPkg>(req, when_done));
        // update last_streamed_log_idx_ = last log index + log size
        if (req->get_type() == msg_type::append_entries_request) {
            myself->set_last_streamed_log_idx(req->get_last_log_idx() + req->log_entries().size());
        } else {
            p_wn( "not a append entry type request here, type: %s", msg_type_to_string( req->get_type() ).c_str());
        }
        p_in("msg to peer %d has been write down, start_log_idx: %ld, size: %ld, pending reqs: %ld", 
        config_->get_id(), req->get_last_log_idx(), req->log_entries().size(), pending_read_reqs_.size());
        myself->write_done();
    }
    
    // handle_write_done(resp, err);
}

bool peer::allow_sending_req() {
    return pending_read_reqs_.size() < 10;
}

void peer::handle_append_entries_type(ptr<peer> myself, 
                                         ptr<req_msg>& req, 
                                         ptr<resp_msg>& resp, 
                                         ptr<rpc_client> my_rpc_client) {
    // trigger another read and try release lock, todo: if we need to serialize all the read
    auto_lock(pending_read_reqs_lock_);
    if (req) {
        auto bi = pending_read_reqs_.begin();
        ptr<req_msg> pending_req = (*bi)->get_req();
        if (req != pending_req) {
            p_wn("req not match, req start_log_idx: %ld, pending req start_log_idx: %ld", 
            req->get_last_log_idx(), pending_req->get_last_log_idx());
        }
    }

    
    if (myself->is_allowed_appending() && resp->get_accepted()) {
        // in appending log (no other request) and resp is accepted
        // even if the write thread set the allow appening to free, due to no lock protection for this flag, it may filp up and down.
        // but eventually it becomes disable
        myself->enable_streaming();
    } else {
        // after set this flag to prevent streaming, because it is not protected by a lock, maybe one or few request are still sent.
        myself->disable_append();
    }
    
    
    pending_read_reqs_.pop_front();
    if (!pending_read_reqs_.empty()) {
        auto bi = pending_read_reqs_.begin();
        ptr<PeerReqPkg> next_req_pkg = (*bi);
        ptr<req_msg> next_req = next_req_pkg->get_req();
        my_rpc_client->async_read_response(next_req, next_req_pkg->get_when_done());
        p_db("trigger next read, start_log_idx: %ld", next_req->get_last_log_idx());
    } else {
        p_db("start to get write lock for peer: %d, lock: %d", myself->get_id(), myself->is_writing());
        if (start_writing()) {
            p_db("release lock for peer: %d", myself->get_id());
            // release lock here, still in streaming mode
            if (is_busy() && is_appending()) {
                try_disable_streaming();
                append_done();
                set_free();
            } else {
                p_wn("lock is free by write thread, peer: %d ", get_id());
            }
            write_done();
        }
    }
}

void peer::try_set_free() {
    std::lock_guard<std::mutex> l(rpc_protector_);
    auto_lock(pending_read_reqs_lock_);
    if (pending_read_reqs_.empty() && is_appending() && is_busy()) {
        p_wn("try set free for peer: %d success, free lock in read callback will skip", get_id());
        try_disable_streaming();
        append_done();
        set_free();
    }
}

void peer::try_disable_streaming() {
    if (!try_disable_append()) {
        disable_streaming();
    }
}

// WARNING:
//   We should have the shared pointer of itself (`myself`)
//   and pointer to RPC client (`my_rpc_client`),
//   for the case when
//     1) this peer is removed before this callback function is invoked. OR
//     2) RPC client has been reset and re-connected.
void peer::handle_rpc_result( ptr<peer> myself,
                              ptr<rpc_client> my_rpc_client,
                              ptr<req_msg>& req,
                              ptr<rpc_result>& pending_result,
                              ptr<resp_msg>& resp,
                              ptr<rpc_exception>& err )
{
    const static std::unordered_set<int> msg_types_to_free( {
        // msg_type::append_entries_request,
        msg_type::install_snapshot_request,
        msg_type::request_vote_request,
        msg_type::pre_vote_request,
        msg_type::leave_cluster_request,
        msg_type::custom_notification_request,
        msg_type::reconnect_request,
        msg_type::priority_change_request
    } );

    if (abandoned_) {
        p_in("peer %d has been shut down, ignore response.", config_->get_id());
        return;
    }

    if (req) {
        p_tr( "resp of req %d -> %d, type %s, %s",
              req->get_src(),
              req->get_dst(),
              msg_type_to_string( req->get_type() ).c_str(),
              (err) ? err->what() : "OK" );
    }

    if (err == nilptr) {
        // Succeeded.
        {   std::lock_guard<std::mutex> l(rpc_protector_);
            // The same as below, freeing busy flag should be done
            // only if the RPC hasn't been changed.
            uint64_t cur_rpc_id = rpc_ ? rpc_->get_id() : 0;
            uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
            if (cur_rpc_id != given_rpc_id) {
                p_wn( "[EDGE CASE] got stale RPC response from %d: "
                      "current %p (%" PRIu64 "), from parameter %p (%" PRIu64 "). "
                      "will ignore this response",
                      config_->get_id(),
                      rpc_.get(),
                      cur_rpc_id,
                      my_rpc_client.get(),
                      given_rpc_id );
                return;
            }
            // WARNING:
            //   `set_free()` should be protected by `rpc_protector_`, otherwise
            //   it may free the peer even though new RPC client is already created.
            if ( msg_types_to_free.find(req->get_type()) != msg_types_to_free.end() ) {
                set_free();
            } else if (req->get_type() == msg_type::append_entries_request) {
                handle_append_entries_type(myself, req, resp, my_rpc_client);
            }
        }

        reset_active_timer();
        {
            auto_lock(lock_);
            resume_hb_speed();
        }
        ptr<rpc_exception> no_except;
        resp->set_peer(myself);
        pending_result->set_result(resp, no_except);

        reconn_backoff_.reset();
        reconn_backoff_.set_duration_ms(1);

    } else {
        // Failed.

        // NOTE: Explicit failure is also treated as an activity
        //       of that connection.
        reset_active_timer();
        {
            auto_lock(lock_);
            slow_down_hb();
        }
        ptr<resp_msg> no_resp;
        // todo: to handle pending request, not just clear
        pending_result->set_result(no_resp, err);

        // Destroy this connection, we MUST NOT re-use existing socket.
        // Next append operation will create a new one.
        {   std::lock_guard<std::mutex> l(rpc_protector_);
            uint64_t cur_rpc_id = rpc_ ? rpc_->get_id() : 0;
            uint64_t given_rpc_id = my_rpc_client ? my_rpc_client->get_id() : 0;
            if (cur_rpc_id == given_rpc_id) {
                rpc_.reset();
                if ( msg_types_to_free.find(req->get_type()) !=
                         msg_types_to_free.end() || req->get_type() == msg_type::append_entries_request) {
                    set_free();
                }

            } else {
                // WARNING (MONSTOR-9378):
                //   RPC client has been reset before this request returns
                //   error. Those two are different instances and we
                //   SHOULD NOT reset the new one.
                p_wn( "[EDGE CASE] RPC for %d has been reset before "
                      "returning error: current %p (%" PRIu64
                      "), from parameter %p (%" PRIu64 ")",
                      config_->get_id(),
                      rpc_.get(),
                      cur_rpc_id,
                      my_rpc_client.get(),
                      given_rpc_id );
            }
        }
    }
}

bool peer::recreate_rpc(ptr<srv_config>& config,
                        context& ctx)
{
    if (abandoned_) {
        p_tr("peer %d is abandoned", config->get_id());
        return false;
    }

    ptr<rpc_client_factory> factory = nullptr;
    {   std::lock_guard<std::mutex> l(ctx.ctx_lock_);
        factory = ctx.rpc_cli_factory_;
    }
    if (!factory) {
        p_tr("client factory is empty");
        return false;
    }

    std::lock_guard<std::mutex> l(rpc_protector_);

    bool backoff_timer_disabled =
        debugging_options::get_instance()
        .disable_reconn_backoff_.load(std::memory_order_relaxed);
    if (backoff_timer_disabled) {
        p_tr("reconnection back-off timer is disabled");
    }

    // To avoid too frequent reconnection attempt,
    // we use exponential backoff (x2) from 1 ms to heartbeat interval.
    if (backoff_timer_disabled || reconn_backoff_.timeout()) {
        reconn_backoff_.reset();
        size_t new_duration_ms = reconn_backoff_.get_duration_us() / 1000;
        new_duration_ms = std::min( hb_interval_, (int32)new_duration_ms * 2 );
        if (!new_duration_ms) new_duration_ms = 1;
        reconn_backoff_.set_duration_ms(new_duration_ms);

        rpc_ = factory->create_client(config->get_endpoint());
        p_tr("%p reconnect peer %d", rpc_.get(), config_->get_id());

        // WARNING:
        //   A reconnection attempt should be treated as an activity,
        //   hence reset timer.
        reset_active_timer();

        reset_streaming();
        set_free();
        set_manual_free();
        return true;

    } else {
        p_tr("skip reconnect this time");
    }
    return false;
}

void peer::shutdown() {
    // Should set the flag to block all incoming requests.
    abandoned_ = true;

    // Cut off all shared pointers related to ASIO and Raft server.
    scheduler_.reset();
    {   // To guarantee atomic reset
        // (race between send_req()).
        std::lock_guard<std::mutex> l(rpc_protector_);
        rpc_.reset();
    }
    hb_task_.reset();
}

} // namespace nuraft;

