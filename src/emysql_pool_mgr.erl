%% Copyright (c) 2016
%% redink <cnredink@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(emysql_pool_mgr).
-include("emysql.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([add_pool/2,
         remove_pool/1,
         has_pool/1,
         get_pool_server/1,
         pools/0,
         conns_for_pool/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(HIBERNATE_TIMEOUT, 10000).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

pools() ->
    case ets:info(?MODULE) of
        undefined ->
            [];
        _ ->
            ets:tab2list(?MODULE)
    end.

conns_for_pool(PoolID) ->
    emysql_conn_mgr:pools(get_pool_server(PoolID)).

has_pool(PoolID) ->
    case ets:lookup(?MODULE, PoolID) of
        [] ->
            false;
        _ ->
            true
    end.

add_pool(PoolID, Pool) ->
    case ets:lookup(?MODULE, PoolID) of
        [] ->
            case supervisor:start_child(emysql_conn_pool_sup, [Pool]) of
                {ok, Pid} ->
                    ets:insert(?MODULE, {PoolID, Pid}),
                    {ok, Pid};
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, exist}
    end.

remove_pool(PoolID) ->
    case ets:lookup(?MODULE, PoolID) of
        [{PoolID, Pid}] ->
            ets:delete(?MODULE, PoolID),
            catch emysql_conn_mgr:stop(Pid);
        _ -> 
            ok
    end.

get_pool_server(PoolID) ->
    case catch ets:lookup(?MODULE, PoolID) of
        [{PoolID, Pid}] ->
            Pid;
        _ ->
            exit("can't find pool server")
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    ets:new(?MODULE, [named_table, public, set, {read_concurrency, true}]),
    erlang:send(erlang:self(), {initialize_pools}),
    {ok, #state{}, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {reply, ok, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({initialize_pools}, State) ->
    %% if the emysql application values are not present in the config      
    %% file we will initialize and empty set of pools. Otherwise, the      
    %% values defined in the config are used to initialize the state.      
    InitializesPools = 
        [      
            {PoolId, #pool{     
                pool_id = PoolId,      
                size = proplists:get_value(size, Props, 1),        
                user = proplists:get_value(user, Props),       
                password = proplists:get_value(password, Props),       
                host = proplists:get_value(host, Props),       
                port = proplists:get_value(port, Props),       
                database = proplists:get_value(database, Props),       
                encoding = proplists:get_value(encoding, Props),       
                start_cmds = proplists:get_value(start_cmds, Props, [])        
            }} || {PoolId, Props} <- emysql_app:pools()     
        ],
    [begin
        case emysql_conn:open_connections(Pool) of
            {ok, Pool1} ->
                emysql_pool_mgr:add_pool(PoolId, Pool1);
            {error, Reason} ->
                erlang:throw(Reason)
        end
    end || {PoolId, Pool} <- InitializesPools],
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(timeout, State) ->
    proc_lib:hibernate(gen_server, enter_loop,
               [?MODULE, [], State]),
    {noreply, State, ?HIBERNATE_TIMEOUT};

handle_info(_Info, State) ->
    {noreply, State, ?HIBERNATE_TIMEOUT}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
