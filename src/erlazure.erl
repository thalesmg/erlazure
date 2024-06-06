%% Copyright (c) 2013 - 2015, Dmitry Kataskin
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% * Redistributions of source code must retain the above copyright notice,
%% this list of conditions and the following disclaimer.
%% * Redistributions in binary form must reproduce the above copyright
%% notice, this list of conditions and the following disclaimer in the
%% documentation and/or other materials provided with the distribution.
%% * Neither the name of erlazure nor the names of its contributors may be used to
%% endorse or promote products derived from this software without specific
%% prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
%% LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
%% CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
%% ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
%% POSSIBILITY OF SUCH DAMAGE.

%% ============================================================================
%% Azure Storage API.
%% ============================================================================

-module(erlazure).
-author("Dmitry Kataskin").

-include("../include/erlazure.hrl").

-define(json_content_type, "application/json").

%% API
-export([new/1, new/2]).

%% Queue API
-export([list_queues/1, list_queues/2]).
-export([set_queue_acl/3, set_queue_acl/4]).
-export([get_queue_acl/2, get_queue_acl/3]).
-export([create_queue/2, create_queue/3]).
-export([delete_queue/2, delete_queue/3]).
-export([put_message/3, put_message/4]).
-export([get_messages/2, get_messages/3]).
-export([peek_messages/2, peek_messages/3]).
-export([delete_message/4, delete_message/5]).
-export([clear_messages/2, clear_messages/3]).
-export([update_message/4, update_message/5]).

%% Blob API
-export([list_containers/1, list_containers/2]).
-export([create_container/2, create_container/3]).
-export([delete_container/2, delete_container/3]).
-export([lease_container/3, lease_container/4]).
-export([list_blobs/2, list_blobs/3]).
-export([put_block_blob/4, put_block_blob/5]).
-export([put_append_blob/3, put_append_blob/4]).
-export([append_block/4, append_block/5]).
-export([put_page_blob/4, put_page_blob/5]).
-export([get_blob/3, get_blob/4]).
-export([snapshot_blob/3, snapshot_blob/4]).
-export([copy_blob/4, copy_blob/5]).
-export([delete_blob/3, delete_blob/4]).
-export([put_block/5, put_block/6]).
-export([put_block_list/4, put_block_list/5]).
-export([get_block_list/3, get_block_list/4]).
-export([acquire_blob_lease/4, acquire_blob_lease/5, acquire_blob_lease/6]).

%% Table API
-export([list_tables/1, list_tables/2, new_table/2, delete_table/2]).

%% Host API
-export([get_host/3]).

-type init_opts() :: #{
    account := string(),
    key := string() | function(),
    endpoint => string()
}.
-type state_opts() :: #{
    endpoint := undefined | string()
}.

-record(state, { account = "", key = "", options = #{}, param_specs = [] }).
-type state() :: #state{}.

-export_types([state/0]).

%%====================================================================
%% API
%%====================================================================

-spec new(init_opts()) -> {ok, state()}.
new(InitOpts0) ->
        InitOpts = ensure_wrapped_key(InitOpts0),
        #{ account := Account
         , key := Key
         } = InitOpts,
        StateOpts = parse_init_opts(InitOpts),
        {ok, #state { account = Account,
                      key = Key,
                      options = StateOpts,
                      param_specs = get_req_param_specs() }}.

-spec new(string(), string()) -> {ok, state()}.
new(Account, Key) ->
        new(#{account => Account, key => Key}).

%%====================================================================
%% Queue
%%====================================================================

-spec list_queues(state()) -> enum_parse_result(queue()).
list_queues(State) ->
        list_queues(State, []).

-spec list_queues(state(), common_opts()) -> enum_parse_result(queue()).
list_queues(State, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{params, [{comp, list}] ++ Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        erlazure_queue:parse_queue_list(Body).

-type queue_acl_opts() :: req_param_timeout() | req_param_clientrequestid().
-spec set_queue_acl(state(), string(), signed_id()) -> {ok, created}.
set_queue_acl(State, Queue, SignedId=#signed_id{}) ->
        set_queue_acl(State, Queue, SignedId, []).

-spec set_queue_acl(state(), string(), signed_id(), list(queue_acl_opts())) -> {ok, created}.
set_queue_acl(State, Queue, SignedId=#signed_id{}, Options) when is_list(Options)->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, put},
                      {path, string:to_lower(Queue)},
                      {body, erlazure_queue:get_request_body(set_queue_acl, SignedId)},
                      {params, [{comp, acl}] ++ Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, created).

-spec get_queue_acl(state(), string()) -> {ok, no_acl} | {ok, signed_id()}.
get_queue_acl(State, Queue) ->
        get_queue_acl(State, Queue, []).

-spec get_queue_acl(state(), string(), list(queue_acl_opts())) -> {ok, no_acl} | {ok, signed_id()}.
get_queue_acl(State, Queue, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{path, string:to_lower(Queue)},
                      {params, [{comp, acl}] ++ Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        erlazure_queue:parse_queue_acl_response(Body).

-spec create_queue(state(), string()) -> created_response() | already_created_response().
create_queue(State, Queue) ->
        create_queue(State, Queue, []).
create_queue(State, Queue, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, put},
                      {path, string:to_lower(Queue)},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {Code, _Body} = execute_request(ServiceContext, ReqContext),
        case Code of
          ?http_created ->
            {ok, created};
          ?http_no_content ->
            {error, already_created}
        end.

delete_queue(State, Queue) ->
        delete_queue(State, Queue, []).
delete_queue(State, Queue, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, delete},
                      {path, string:to_lower(Queue)},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, deleted).

put_message(State, Queue, Message) ->
        put_message(State, Queue, Message, []).
put_message(State, Queue, Message, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, post},
                      {path, lists:concat([string:to_lower(Queue), "/messages"])},
                      {body, erlazure_queue:get_request_body(put_message, Message)},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created).

get_messages(State, Queue) ->
        get_messages(State, Queue, []).
get_messages(State, Queue, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{path, string:to_lower(Queue) ++ "/messages"},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        erlazure_queue:parse_queue_messages_list(Body).

peek_messages(State, Queue) ->
        peek_messages(State, Queue, []).
peek_messages(State, Queue, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{path, string:to_lower(Queue) ++ "/messages"},
                      {params, [{peek_only, true}] ++ Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        erlazure_queue:parse_queue_messages_list(Body).

delete_message(State, Queue, MessageId, PopReceipt) ->
        delete_message(State, Queue, MessageId, PopReceipt, []).
delete_message(State, Queue, MessageId, PopReceipt, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, delete},
                      {path, lists:concat([string:to_lower(Queue), "/messages/", MessageId])},
                      {params, [{pop_receipt, PopReceipt}] ++ Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, deleted).

clear_messages(State, Queue) ->
        clear_messages(State, Queue, []).
clear_messages(State, Queue, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, delete},
                      {path, string:to_lower(Queue) ++ "/messages"},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, deleted).

update_message(State, Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout) ->
        update_message(State, Queue, UpdatedMessage, VisibilityTimeout, []).
update_message(State, Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?queue_service, State),
        Params = [{pop_receipt, UpdatedMessage#queue_message.pop_receipt},
                  {message_visibility_timeout, integer_to_list(VisibilityTimeout)}],
        ReqOptions = [{method, put},
                      {path, lists:concat([string:to_lower(Queue), "/messages/", UpdatedMessage#queue_message.id])},
                      {body, erlazure_queue:get_request_body(update_message, UpdatedMessage#queue_message.text)},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?queue_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, updated).

%%====================================================================
%% Blob
%%====================================================================

list_containers(State) ->
        list_containers(State, []).
list_containers(State, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{params, [{comp, list}] ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        case execute_request(ServiceContext, ReqContext) of
            {?http_ok, Body} ->
                {ok, Containers} = erlazure_blob:parse_container_list(Body),
                Containers;
            {error, _} = Error ->
                Error
        end.

create_container(State, Name) ->
        create_container(State, Name, []).
create_container(State, Name, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, Name},
                      {params, [{res_type, container}] ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),
        {Code, Body} = execute_request(ServiceContext, ReqContext),
        case Code of
          ?http_created -> {ok, created};
          _ -> {error, Body}
        end.

delete_container(State, Name) ->
        delete_container(State, Name, []).
delete_container(State, Name, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, delete},
                      {path, Name},
                      {params, [{res_type, container}] ++ Options}],
        RequestContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, RequestContext),
        return_response(Code, Body, State, ?http_accepted, deleted).

put_block_blob(State, Container, Name, Data) ->
        put_block_blob(State, Container, Name, Data, []).
put_block_blob(State, Container, Name, Data, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Name])},
                      {body, Data},
                      {params, [{blob_type, block_blob}] ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),
        ReqContext1 = case proplists:get_value(content_type, Options) of
                        undefined    -> ReqContext#req_context{ content_type = "application/octet-stream" };
                        ContentType  -> ReqContext#req_context{ content_type = ContentType }
                      end,

        {Code, Body} = execute_request(ServiceContext, ReqContext1),
        return_response(Code, Body, State, ?http_created, created).

put_page_blob(State, Container, Name, ContentLength) ->
        put_page_blob(State, Container, Name, ContentLength, []).
put_page_blob(State, Container, Name, ContentLength, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{blob_type, page_blob},
                  {blob_content_length, ContentLength}],
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Name])},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created).

put_append_blob(State, Container, Name) ->
        put_append_blob(State, Container, Name, []).
put_append_blob(State, Container, Name, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{blob_type, append_blob}],
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Name])},
                      {params, Params ++ Options}],
        ReqContext1 = new_req_context(?blob_service, ReqOptions, State),
        ReqContext = case proplists:get_value(content_type, Options) of
                       undefined    -> ReqContext1#req_context{ content_type = "application/octet-stream" };
                       ContentType  -> ReqContext1#req_context{ content_type = ContentType }
                     end,

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created).

append_block(State, Container, Name, Data) ->
        append_block(State, Container, Name, Data, []).
append_block(State, Container, Name, Data, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{comp, "appendblock"}],
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Name])},
                      {body, Data},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, appended).

list_blobs(State, Container) ->
        list_blobs(State, Container, []).
list_blobs(State, Container, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{comp, list},
                  {res_type, container}],
        ReqOptions = [{path, Container},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {ok, Blobs} = erlazure_blob:parse_blob_list(Body),
        Blobs.

get_blob(State, Container, Blob) ->
        get_blob(State, Container, Blob, []).
get_blob(State, Container, Blob, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{path, lists:concat([Container, "/", Blob])},
                      {params, Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        case Code of
          ?http_ok ->
            {ok, Body};
          ?http_partial_content->
            {ok, Body};
          _ -> {error, Body}
        end.

snapshot_blob(State, Container, Blob) ->
        snapshot_blob(State, Container, Blob, []).
snapshot_blob(State, Container, Blob, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {params, [{comp, snapshot}] ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created).

copy_blob(State, Container, Blob, Source) ->
        copy_blob(State, Container, Blob, Source, []).
copy_blob(State, Container, Blob, Source, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {params, [{blob_copy_source, Source}] ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_accepted, created).

delete_blob(State, Container, Blob) ->
        delete_blob(State, Container, Blob, []).
delete_blob(State, Container, Blob, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, delete},
                      {path, lists:concat([Container, "/", Blob])},
                      {params, Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_accepted, deleted).

put_block(State, Container, Blob, BlockId, BlockContent) ->
        put_block(State, Container, Blob, BlockId, BlockContent, []).
put_block(State, Container, Blob, BlockId, BlockContent, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{comp, block},
                  {blob_block_id, base64:encode_to_string(BlockId)}],
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {body, BlockContent},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created).

put_block_list(State, Container, Blob, BlockRefs) ->
        put_block_list(State, Container, Blob, BlockRefs, []).
put_block_list(State, Container, Blob, BlockRefs, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {body, erlazure_blob:get_request_body(BlockRefs)},
                      {params, [{comp, "blocklist"}] ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created).

get_block_list(State, Container, Blob) ->
        get_block_list(State, Container, Blob, []).
get_block_list(State, Container, Blob, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{path, lists:concat([Container, "/", Blob])},
                      {params, [{comp, "blocklist"}] ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {ok, BlockList} = erlazure_blob:parse_block_list(Body),
        BlockList.

acquire_blob_lease(State, Container, Blob, Duration) ->
        acquire_blob_lease(State, Container, Blob, Duration, []).
acquire_blob_lease(State, Container, Blob, Duration, Options) ->
        acquire_blob_lease(State, Container, Blob, "", Duration, Options).
acquire_blob_lease(State, Container, Blob, ProposedId, Duration, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),

        Params = [{lease_action, acquire},
                  {proposed_lease_id, ProposedId},
                  {lease_duration, Duration},
                  {comp, lease}],

        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, acquired).

lease_container(State, Name, Mode) ->
        lease_container(State, Name, Mode, []).
lease_container(State, Name, Mode, Options) when is_atom(Mode), is_list(Options) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{comp, lease},
                  {res_type, container},
                  {lease_action, Mode}],
        ReqOptions = [{method, put},
                      {path, Name},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, ReqOptions, State),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_accepted, deleted).

%%====================================================================
%% Table
%%====================================================================

list_tables(State) ->
        list_tables(State, []).
list_tables(State, Options) when is_list(Options) ->
        ServiceContext = new_service_context(?table_service, State),
        ReqOptions = [{path, "Tables"},
                      {params, Options}],
        ReqContext = new_req_context(?table_service, ReqOptions, State),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {ok, erlazure_table:parse_table_list(Body)}.

new_table(State, TableName) when is_list(TableName) ->
        new_table(State, list_to_binary(TableName));

new_table(State, TableName) when is_binary(TableName) ->
        ServiceContext = new_service_context(?table_service, State),
        ReqOptions = [{path, "Tables"},
                      {method, post},
                      {body, jsx:encode([{<<"TableName">>, TableName}])}],
        ReqContext = new_req_context(?table_service, ReqOptions, State),
        ReqContext1 = ReqContext#req_context{ content_type = ?json_content_type },
        {Code, Body} = execute_request(ServiceContext, ReqContext1),
        return_response(Code, Body, State, ?http_created, created).

delete_table(State, TableName) when is_binary(TableName) ->
        delete_table(State, binary_to_list(TableName));

delete_table(State, TableName) when is_list(TableName) ->
        ServiceContext = new_service_context(?table_service, State),
        ReqOptions = [{path, io:format("Tables('~s')", [TableName])},
                      {method, delete}],
        ReqContext = new_req_context(?table_service, ReqOptions, State),
        {?http_no_content, _} = execute_request(ServiceContext, ReqContext),
        {ok, deleted}.

%%--------------------------------------------------------------------
%% Private functions
%%--------------------------------------------------------------------

-spec execute_request(service_context(), req_context()) -> {non_neg_integer(), binary()}.
execute_request(ServiceContext = #service_context{}, ReqContext = #req_context{}) ->
        DateHeader = if (ServiceContext#service_context.service =:= ?table_service) ->
                          {"Date", httpd_util:rfc1123_date()};
                        true ->
                          {"x-ms-date", httpd_util:rfc1123_date()}
                     end,

        Headers =  [DateHeader,
                    {"x-ms-version", ServiceContext#service_context.api_version},
                    {"Host", get_host(ServiceContext#service_context.service,
                                      ServiceContext#service_context.account)}],

        Headers1 = if (ReqContext#req_context.method =:= put orelse
                       ReqContext#req_context.method =:= post) ->
                        ContentHeaders = [{"Content-Type", ReqContext#req_context.content_type},
                                          {"Content-Length", integer_to_list(ReqContext#req_context.content_length)}],
                        lists:append([Headers, ContentHeaders, ReqContext#req_context.headers]);

                      true ->
                        lists:append([Headers, ReqContext#req_context.headers])
                   end,

        AuthHeader = {"Authorization", get_shared_key(ServiceContext#service_context.service,
                                                      ServiceContext#service_context.account,
                                                      ServiceContext#service_context.key,
                                                      ReqContext#req_context.method,
                                                      ReqContext#req_context.path,
                                                      ReqContext#req_context.parameters,
                                                      Headers1)},

        %% Fiddler
        %% httpc:set_options([{ proxy, {{"localhost", 9999}, []}}]),

        Response = httpc:request(ReqContext#req_context.method,
                                 erlazure_http:create_request(ReqContext, [AuthHeader | Headers1]),
                                 [{version, "HTTP/1.1"}, {ssl, [{versions, ['tlsv1.2']}]}],
                                 [{sync, true}, {body_format, binary}, {headers_as_is, true}]),
        case Response of
          {ok, {{_, Code, _}, _, Body}}
          when Code >= 200, Code =< 206 ->
            {Code, Body};

          {ok, {{_, _, _}, _, Body}} ->
            get_error_code(Body);

          {error, Error} ->
            {error, Error}
        end.

get_error_code(Body) ->
         try do_get_error_code(Body) of
             ErrorCodeContext -> {error, ErrorCodeContext}
         catch
             _:_ -> {error, #{raw => Body}}
         end.

do_get_error_code(Body) ->
        {ParseResult, _} = xmerl_scan:string(binary_to_list(Body)),
        ErrorContent = ParseResult#xmlElement.content,
        Code =
            lists:flatten([Txt
                           || #xmlElement{name = 'Code', content = Cs} <- ErrorContent,
                              #xmlText{value = Txt} <- Cs]),
        Message =
            lists:flatten([Txt
                           || #xmlElement{name = 'Message', content = Cs} <- ErrorContent,
                              #xmlText{value = Txt} <- Cs]),
        #{code => Code, message => Message}.

get_shared_key(Service, Account, Key, HttpMethod, Path, Parameters, Headers) ->
        SignatureString = get_signature_string(Service, HttpMethod, Headers, Account, Path, Parameters),
        "SharedKey " ++ Account ++ ":" ++ base64:encode_to_string(sign_string(Key, SignatureString)).

get_signature_string(Service, HttpMethod, Headers, Account, Path, Parameters) ->
        SigStr1 = erlazure_http:verb_to_str(HttpMethod) ++ "\n" ++
                  get_headers_string(Service, Headers),

        SigStr2 = if (Service =:= ?queue_service) orelse (Service =:= ?blob_service) ->
                    SigStr1 ++ canonicalize_headers(Headers);
                    true -> SigStr1
                  end,
        SigStr2 ++ canonicalize_resource(Account, Path, Parameters).

get_headers_string(Service, Headers) ->
        FoldFun = fun(HeaderName, Acc) ->
                    case lists:keyfind(HeaderName, 1, Headers) of
                      %% Special case: zero length should be an empty line.
                      {"Content-Length", "0"} -> lists:concat([Acc, "\n"]);
                      {HeaderName, Value} -> lists:concat([Acc, Value, "\n"]);
                      false -> lists:concat([Acc, "\n"])
                    end
                  end,
        lists:foldl(FoldFun, "", get_header_names(Service)).

-ifdef(OTP_RELEASE).
        %% OTP 23 or higher
        -if(?OTP_RELEASE >= 23).
                hmac(Key, Str) ->
                        crypto:mac(hmac, sha256, Key, Str).
        -else.
                hmac(Key, Str) ->
                        crypto:hmac(sha256, Key, Str).
        -endif.
-else.
        %% OTP 20 or lower.
        hmac(Key, Str) ->
                crypto:hmac(sha256, Key, Str).
-endif

-spec sign_string(base64:ascii_string(), string()) -> binary().
sign_string(Key, StringToSign) ->
        hmac(base64:decode(unwrap(Key)), StringToSign).

build_uri_base(_Service, #state{options = #{endpoint := Endpoint}}) when Endpoint =/= undefined ->
        Endpoint;
build_uri_base(Service, #state{account = Account}) ->
        lists:concat(["https://", get_host(Service, Account), "/"]).

get_host(Service, Account) ->
        lists:concat([Account, ".", erlang:atom_to_list(Service), ".core.windows.net"]).

get_host(State, Service, []) ->
        Domain  = ".core.windows.net",
        do_get_host(State, Service, Domain);
get_host(State, Service, Timeout) when is_integer(Timeout) ->
        Domain  = ".core.windows.net",
        do_get_host(State, Service, Domain);
get_host(State, Service, Domain) when is_list(Domain) ->
        do_get_host(State, Service, Domain).

do_get_host(State, Service, Domain) when is_list(Domain), is_atom(Service) ->
        Account = State#state.account,
        lists:concat([Account, ".", atom_to_list(Service), Domain]).

-spec canonicalize_headers([string()]) -> string().
canonicalize_headers(Headers) ->
        MSHeaderNames = [HeaderName || {HeaderName, _} <- Headers, string:str(HeaderName, "x-ms-") =:= 1],
        SortedHeaderNames = lists:sort(MSHeaderNames),
        FoldFun = fun(HeaderName, Acc) ->
                    {_, Value} = lists:keyfind(HeaderName, 1, Headers),
                    lists:concat([Acc, HeaderName, ":", Value, "\n"])
                  end,
        lists:foldl(FoldFun, "", SortedHeaderNames).

canonicalize_resource(Account, Path, []) ->
        lists:concat(["/", Account, "/", Path]);

canonicalize_resource(Account, Path, Parameters) ->
        SortFun = fun({ParamNameA, ParamValA}, {ParamNameB, ParamValB}) ->
                    ParamNameA ++ ParamValA =< ParamNameB ++ ParamValB
                 end,
        SortedParameters = lists:sort(SortFun, Parameters),
        [H | T] = SortedParameters,
        "/" ++ Account ++ "/" ++ Path ++ combine_canonical_param(H, "", "", T).

combine_canonical_param({Param, Value}, Param, Acc, []) ->
        add_value(Value, Acc);

combine_canonical_param({Param, Value}, _PreviousParam, Acc, []) ->
        add_param_value(Param, Value, Acc);

combine_canonical_param({Param, Value}, Param, Acc, ParamList) ->
        [H | T] = ParamList,
        combine_canonical_param(H, Param, add_value(Value, Acc), T);

combine_canonical_param({Param, Value}, _PreviousParam, Acc, ParamList) ->
        [H | T] = ParamList,
        combine_canonical_param(H, Param, add_param_value(Param, Value, Acc), T).

add_param_value(Param, Value, Acc) ->
        Acc ++ "\n" ++ string:to_lower(Param) ++ ":" ++ Value.

add_value(Value, Acc) ->
        Acc ++ "," ++ Value.

get_header_names(?blob_service) ->
        get_header_names(?queue_service);

get_header_names(?queue_service) ->
        ["Content-Encoding",
         "Content-Language",
         "Content-Length",
         "Constent-MD5",
         "Content-Type",
         "Date",
         "If-Modified-Since",
         "If-Match",
         "If-None-Match",
         "If-Unmodified-Since",
         "Range"];

get_header_names(?table_service) ->
        ["Content-MD5",
         "Content-Type",
         "Date"].

new_service_context(?queue_service, State=#state{}) ->
        #service_context{ service = ?queue_service,
                          api_version = ?queue_service_ver,
                          account = State#state.account,
                          key = State#state.key };

new_service_context(?blob_service, State=#state{}) ->
        #service_context{ service = ?blob_service,
                          api_version = ?blob_service_ver,
                          account = State#state.account,
                          key = State#state.key };

new_service_context(?table_service, State=#state{}) ->
        #service_context{ service = ?table_service,
                          api_version = ?table_service_ver,
                          account = State#state.account,
                          key = State#state.key }.

new_req_context(Service, Options, State) ->
        Method = proplists:get_value(method, Options, get),
        Path = proplists:get_value(path, Options, ""),
        Body = proplists:get_value(body, Options, ""),
        Headers = proplists:get_value(headers, Options, []),
        Params = proplists:get_value(params, Options, []),
        AddHeaders = if (Service =:= ?table_service) ->
                        case lists:keyfind("Accept", 1, Headers) of
                          false -> [{"Accept", "application/json;odata=fullmetadata"}];
                          _ -> []
                        end;
                        true -> []
                     end,

        ParamSpecs = State#state.param_specs,
        ReqParams = get_req_uri_params(Params, ParamSpecs),
        ReqHeaders = lists:append([Headers, AddHeaders, get_req_headers(Params, ParamSpecs)]),

        #req_context{ address = build_uri_base(Service, State),
                      path = Path,
                      method = Method,
                      body = Body,
                      content_length = erlazure_http:get_content_length(Body),
                      parameters = ReqParams,
                      headers = ReqHeaders }.

get_req_headers(Params, ParamSpecs) ->
        get_req_params(Params, ParamSpecs, header).

get_req_uri_params(Params, ParamSpecs) ->
        get_req_params(Params, ParamSpecs, uri).

get_req_params(Params, ParamSpecs, Type) ->
        ParamDefs = orddict:filter(fun(_, Value) -> Value#param_spec.type =:= Type end, ParamSpecs),
        FoldFun = fun({_ParamName, ""}, Acc) ->
                      Acc;

                      ({ParamName, ParamValue}, Acc) ->
                        case orddict:find(ParamName, ParamDefs) of
                          {ok, Value} -> [{Value#param_spec.name, (Value#param_spec.parse_fun)(ParamValue)} | Acc];
                          error -> Acc
                        end
                  end,
        lists:foldl(FoldFun, [], Params).

get_req_param_specs() ->
        ProcessFun = fun(Spec=#param_spec{}, Dictionary) ->
                        orddict:store(Spec#param_spec.id, Spec, Dictionary)
                    end,

        CommonParamSpecs = lists:foldl(ProcessFun, orddict:new(), get_req_common_param_specs()),
        BlobParamSpecs = lists:foldl(ProcessFun, CommonParamSpecs, erlazure_blob:get_request_param_specs()),

        lists:foldl(ProcessFun, BlobParamSpecs, erlazure_queue:get_request_param_specs()).

get_req_common_param_specs() ->
        [#param_spec{ id = comp, type = uri, name = "comp" },
         #param_spec{ id = ?req_param_timeout, type = uri, name = "timeout" },
         #param_spec{ id = ?req_param_maxresults, type = uri, name = "maxresults" },
         #param_spec{ id = ?req_param_prefix, type = uri, name = "prefix" },
         #param_spec{ id = ?req_param_include, type = uri, name = "include" },
         #param_spec{ id = ?req_param_marker, type = uri, name = "marker" }].

return_response(Code, Body, State, ExpectedResponseCode, SuccessAtom) ->
  case Code of
    ExpectedResponseCode ->
      {ok, SuccessAtom};
    _ ->
      {error, Body}
  end.

-spec parse_init_opts(init_opts()) -> state_opts().
parse_init_opts(InitOpts) ->
    Endpoint = maps:get(endpoint, InitOpts, undefined),
    #{ endpoint => Endpoint
     }.

unwrap(Fun) when is_function(Fun) ->
    %% handle potentially nested functions
    unwrap(Fun());
unwrap(V) ->
    V.

wrap(V) ->
    fun() ->
            V
    end.

-spec ensure_wrapped_key(init_opts()) -> init_opts().
ensure_wrapped_key(#{key := Key} = InitOpts) ->
    case is_function(Key) of
        true ->
            InitOpts;
        false ->
            InitOpts#{key := wrap(Key)}
    end.

%%====================================================================
%% Tests
%%====================================================================
-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

sample_error_raw() ->
    <<"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<Error>\n  <Code>AuthorizationFailure</Code>\n  <Message>Server failed to authenticate the request. Make sure the value of the Authorization header is formed correctly including the signature.\nRequestId:9d2010f6-fe5f-4cc0-ba45-54162b64e1c9\nTime:2024-05-08T14:53:22.751Z</Message>\n</Error>">>.

get_error_code_test_() ->
    [ { "sample error response",
        ?_assertMatch(
          {error, #{ code := "AuthorizationFailure"
                   , message := "Server failed to authenticate" ++ _
                   }},
          get_error_code(sample_error_raw())
        )
      }
    , { "unparseable error"
      , ?_assertMatch(
          {error, #{raw := <<"something else">>}},
          get_error_code(<<"something else">>)
        )
      }
    ].

%% END ifdef(TEST)
-endif.
