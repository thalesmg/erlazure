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

start(Config) ->
    Endpoint = ?config(endpoint, Config),
    {ok, Pid} = erlazure:start(#{account => ?ACCOUNT, key => ?KEY, endpoint => Endpoint}),
    Pid.

delete_all_containers(Config) ->
    Pid = start(Config),
    {Containers, _} = erlazure:list_containers(Pid),
    lists:foreach(
      fun(#blob_container{name = Name}) ->
        {ok, deleted} = erlazure:delete_container(Pid, Name)
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
    {ok, Pid} = erlazure:start(#{account => ?ACCOUNT, key => ?KEY, endpoint => Endpoint}),
    %% Create a container
    Container = container_name(?FUNCTION_NAME),
    ?assertMatch({[], _}, erlazure:list_containers(Pid)),
    ?assertMatch({ok, created}, erlazure:create_container(Pid, Container)),
    %% Upload some blobs
    ?assertMatch({ok, created}, erlazure:put_block_blob(Pid, Container, "blob1", <<"1">>)),
    ?assertMatch({ok, created}, erlazure:put_block_blob(Pid, Container, "blob2", <<"2">>)),
    ?assertMatch({[#cloud_blob{name = "blob1"}, #cloud_blob{name = "blob2"}], _},
                 erlazure:list_blobs(Pid, Container)),
    %% Read back data
    ?assertMatch({ok, <<"1">>}, erlazure:get_blob(Pid, Container, "blob1")),
    ?assertMatch({ok, <<"2">>}, erlazure:get_blob(Pid, Container, "blob2")),
    %% Delete blob
    ?assertMatch({ok, deleted}, erlazure:delete_blob(Pid, Container, "blob1")),
    ?assertMatch({[#cloud_blob{name = "blob2"}], _},
                 erlazure:list_blobs(Pid, Container)),
    %% Delete container
    ?assertMatch({ok, deleted}, erlazure:delete_container(Pid, Container)),
    ok.

%% Basic smoke test to check that we can pass already wrapped keys to `erlazure:start`.
t_blob_storage_wrapped_key(Config) ->
    Endpoint = ?config(endpoint, Config),
    {ok, Pid} = erlazure:start(#{account => ?ACCOUNT, key => ?KEY, endpoint => Endpoint}),
    ?assertMatch({[], _}, erlazure:list_containers(Pid)),
    ok.
