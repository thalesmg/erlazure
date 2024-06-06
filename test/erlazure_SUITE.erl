%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(erlazure_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("erlazure.hrl").

%%------------------------------------------------------------------------------
%% Type definitions
%%------------------------------------------------------------------------------

%% Default Azurite credentials
%% See: https://github.com/Azure/Azurite/blob/main/README.md#default-storage-account
-define(ACCOUNT, "devstoreaccount1").
-define(KEY, "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    test_utils:all(?MODULE).

init_per_suite(Config) ->
    Endpoint = os:getenv("AZURITE_ENDPOINT", "http://127.0.0.1:10000/"),
    #{host := Host, port := Port} = uri_string:parse(Endpoint),
    case test_utils:is_tcp_server_available(Host, Port) of
        false ->
            throw(endpoint_unavailable);
        true ->
            ok
    end,
    [{endpoint, Endpoint} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    delete_all_containers(Config),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

new(Config) ->
    Endpoint = ?config(endpoint, Config),
    {ok, State} = erlazure:new(#{account => ?ACCOUNT, key => ?KEY, endpoint => Endpoint}),
    State.

delete_all_containers(Config) ->
    State = new(Config),
    {Containers, _} = erlazure:list_containers(State),
    lists:foreach(
      fun(#blob_container{name = Name}) ->
        {ok, deleted} = erlazure:delete_container(State, Name)
      end,
      Containers).

container_name(Name) ->
    IOList = re:replace(atom_to_list(Name), <<"[^a-z0-9-]">>, <<"-">>, [global]),
    binary_to_list(iolist_to_binary(IOList)).

%%------------------------------------------------------------------------------
%% Test cases : blob storage
%%------------------------------------------------------------------------------

%% Basic smoke test for basic blob storage operations.
t_blob_storage_smoke_test(Config) ->
    Endpoint = ?config(endpoint, Config),
    {ok, State} = erlazure:new(#{account => ?ACCOUNT, key => ?KEY, endpoint => Endpoint}),
    %% Create a container
    Container = container_name(?FUNCTION_NAME),
    ?assertMatch({[], _}, erlazure:list_containers(State)),
    ?assertMatch({ok, created}, erlazure:create_container(State, Container)),
    %% Upload some blobs
    ?assertMatch({ok, created}, erlazure:put_block_blob(State, Container, "blob1", <<"1">>)),
    ?assertMatch({ok, created}, erlazure:put_block_blob(State, Container, "blob2", <<"2">>)),
    ?assertMatch({[#cloud_blob{name = "blob1"}, #cloud_blob{name = "blob2"}], _},
                 erlazure:list_blobs(State, Container)),
    %% Read back data
    ?assertMatch({ok, <<"1">>}, erlazure:get_blob(State, Container, "blob1")),
    ?assertMatch({ok, <<"2">>}, erlazure:get_blob(State, Container, "blob2")),
    %% Delete blob
    ?assertMatch({ok, deleted}, erlazure:delete_blob(State, Container, "blob1")),
    ?assertMatch({[#cloud_blob{name = "blob2"}], _},
                 erlazure:list_blobs(State, Container)),
    %% Delete container
    ?assertMatch({ok, deleted}, erlazure:delete_container(State, Container)),
    ok.

%% Basic smoke test to check that we can pass already wrapped keys to `erlazure:start`.
t_blob_storage_wrapped_key(Config) ->
    Endpoint = ?config(endpoint, Config),
    {ok, State} = erlazure:new(#{account => ?ACCOUNT, key => ?KEY, endpoint => Endpoint}),
    ?assertMatch({[], _}, erlazure:list_containers(State)),
    ok.

%% Basic smoke test for append blob storage operations.
t_append_blob_smoke_test(Config) ->
    Endpoint = ?config(endpoint, Config),
    {ok, State} = erlazure:new(#{account => ?ACCOUNT, key => ?KEY, endpoint => Endpoint}),
    %% Create a container
    Container = container_name(?FUNCTION_NAME),
    ?assertMatch({[], _}, erlazure:list_containers(State)),
    ?assertMatch({ok, created}, erlazure:create_container(State, Container)),
    %% Upload some blobs
    Opts = [{content_type, "text/csv"}],
    ?assertMatch({ok, created}, erlazure:put_append_blob(State, Container, "blob1", Opts)),
    ?assertMatch({ok, appended}, erlazure:append_block(State, Container, "blob1", <<"1">>)),
    ?assertMatch({ok, appended}, erlazure:append_block(State, Container, "blob1", <<"\n">>)),
    ?assertMatch({ok, appended}, erlazure:append_block(State, Container, "blob1", <<"2">>)),
    ListedBlobs = erlazure:list_blobs(State, Container),
    ?assertMatch({[#cloud_blob{name = "blob1"}], _},
                 ListedBlobs),
    {[#cloud_blob{name = "blob1", properties = BlobProps}], _} = ListedBlobs,
    ?assertMatch(#{content_type := "text/csv"}, maps:from_list(BlobProps)),
    %% Read back data
    ?assertMatch({ok, <<"1\n2">>}, erlazure:get_blob(State, Container, "blob1")),
    %% Delete blob
    ?assertMatch({ok, deleted}, erlazure:delete_blob(State, Container, "blob1")),
    ?assertMatch({[], _}, erlazure:list_blobs(State, Container)),
    %% Delete container
    ?assertMatch({ok, deleted}, erlazure:delete_container(State, Container)),
    ok.

%% Test error handling when endpoint is unavailable
t_blob_failure_to_connect(_Config) ->
    BadEndpoint = "http://127.0.0.2:65535/",
    {ok, State} = erlazure:new(#{account => ?ACCOUNT, key => ?KEY, endpoint => BadEndpoint}),
    ?assertMatch({error, {failed_connect, _}}, erlazure:list_containers(State)),
    ?assertMatch({error, {failed_connect, _}}, erlazure:create_container(State, "c")),
    ?assertMatch({error, {failed_connect, _}}, erlazure:delete_container(State, "c")),
    ?assertMatch({error, {failed_connect, _}}, erlazure:put_append_blob(State, "c", "b1")),
    ?assertMatch({error, {failed_connect, _}}, erlazure:put_block_blob(State, "c", "b1", <<"a">>)),
    ?assertMatch({error, {failed_connect, _}}, erlazure:append_block(State, "c", "b1", <<"a">>)),
    ?assertMatch({error, {failed_connect, _}}, erlazure:get_blob(State, "c", "b1")),
    ok.

%% Basic smoke test for block blob storage operations.
t_put_block(Config) ->
    Endpoint = ?config(endpoint, Config),
    {ok, State} = erlazure:new(#{account => ?ACCOUNT, key => ?KEY, endpoint => Endpoint}),
    %% Create a container
    Container = container_name(?FUNCTION_NAME),
    ?assertMatch({[], _}, erlazure:list_containers(State)),
    ?assertMatch({ok, created}, erlazure:create_container(State, Container)),
    %% Upload some blocks.  Note: this content-type will be overwritten later by `put_block_list'.
    Opts1 = [{content_type, "application/json"}],
    BlobName = "blob1",
    ?assertMatch({ok, created}, erlazure:put_block_blob(State, Container, BlobName, <<"0">>, Opts1)),
    %% Note: this short name is important for this test.  It'll produce a base64 string
    %% that's padded.  That padding must be URL-encoded when sending the request, but not
    %% when generating the string to sign.
    BlockId1 = <<"blo1">>,
    ?assertMatch({ok, created}, erlazure:put_block(State, Container, BlobName, BlockId1, <<"a">>)),
    %% Testing iolists
    BlockId2 = <<"blo2">>,
    ?assertMatch({ok, created}, erlazure:put_block(State, Container, BlobName, BlockId2, [<<"\n">>, ["b", [$\n]]])),
    %% Not yet committed.  Contains only the data from the blob creation.
    ?assertMatch({ok, <<"0">>}, erlazure:get_blob(State, Container, BlobName)),
    %% Committing
    BlockList1 = [{BlockId1, latest}],
    ?assertMatch({ok, created}, erlazure:put_block_list(State, Container, BlobName, BlockList1)),
    %% Committed only first block.  Initial data was lost, as it was not in the block list.
    ?assertMatch({ok, <<"a">>}, erlazure:get_blob(State, Container, BlobName)),
    %% Block 2 was dropped after committing.
    ?assertMatch({[#blob_block{id = "blo1"}], _}, erlazure:get_block_list(State, Container, BlobName)),
    BlockId3 = <<"blo3">>,
    ?assertMatch({ok, created}, erlazure:put_block(State, Container, BlobName, BlockId3, [<<"\n">>, ["b", [$\n]]])),
    %% Commit both blocks
    Opts2 = [{req_opts, [{headers, [{"x-ms-blob-content-type", "text/csv"}]}]}],
    BlockList2 = [{BlockId1, committed}, {BlockId3, uncommitted}],
    ?assertMatch({ok, created}, erlazure:put_block_list(State, Container, BlobName, BlockList2, Opts2)),
    ?assertMatch({ok, <<"a\nb\n">>}, erlazure:get_blob(State, Container, BlobName)),
    %% Check content type.
    ListedBlobs = erlazure:list_blobs(State, Container),
    ?assertMatch({[#cloud_blob{name = "blob1"}], _},
                 ListedBlobs),
    {[#cloud_blob{name = "blob1", properties = Props}], _} = ListedBlobs,
    %% Content-type from `put_block_list' prevails.
    ?assertMatch(#{content_type := "text/csv"}, maps:from_list(Props)),
    %% Delete container
    ?assertMatch({ok, deleted}, erlazure:delete_container(State, Container)),
    ok.
