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

-module(test_utils).
-author("Dmitry Kataskin").

-compile(export_all).
-compile(nowarn_export_all).

-define(DEFAULT_TCP_SERVER_CHECK_AVAIL_TIMEOUT, 1000).

all(Module) ->
    lists:usort([
        F
     || {F, 1} <- Module:module_info(exports),
        string:substr(atom_to_list(F), 1, 2) == "t_"
    ]).

-spec is_tcp_server_available(
    Host :: inet:socket_address() | inet:hostname(),
    Port :: inet:port_number()
) -> boolean.
is_tcp_server_available(Host, Port) ->
    is_tcp_server_available(Host, Port, ?DEFAULT_TCP_SERVER_CHECK_AVAIL_TIMEOUT).

-spec is_tcp_server_available(
    Host :: inet:socket_address() | inet:hostname(),
    Port :: inet:port_number(),
    Timeout :: integer()
) -> boolean.
is_tcp_server_available(Host, Port, Timeout) ->
    case gen_tcp:connect(Host, Port, [], Timeout) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            true;
        {error, _} ->
            false
    end.

append_ticks(Name) ->
        Name ++ integer_to_list(get_ticks()).

get_ticks() ->
        {Mega, Sec, Micro} = now(),
        Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.

read_file(FileName) ->
        {ok, Binary} = file:read_file(file_path(FileName)),
        erlang:binary_to_list(Binary).

file_path(File) ->
        filename:join([code:priv_dir(erlazure), responses, File]).
