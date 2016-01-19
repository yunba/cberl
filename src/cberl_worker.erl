-module(cberl_worker).
-behaviour(poolboy_worker).
-include("cberl.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1, stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link(Args) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

stop(Pid) ->
    gen_server:call(Pid, stop).

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
init([{host, Host}, {username, Username}, {password, Password},
      {bucketname, BucketName}, {transcoder, Transcoder}]) ->
    process_flag(trap_exit, true),
    {ok, Handle} = cberl_nif:new(),
    State = #instance{handle = Handle,
                      transcoder = Transcoder,
                      bucketname = canonical_bucket_name(BucketName),
                      opts = [Host, Username, Password, BucketName],
                      connected = false},
    State2 = case connect(State) of
        ok -> State#instance{connected = true};
        {error, _} -> State#instance{connected = false}
    end,
    {ok, State2}.

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
handle_call({mtouch, Keys, ExpTimesE}, _From, State) ->
    {Connected, Reply} = case connect(State) of
        ok -> {true, mtouch(Keys, ExpTimesE, State)};
        {error, _} -> {false, {error, unavailable}}
    end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call({unlock, Key, Cas}, _From, State) ->
    {Connected, Reply} = case connect(State) of
        ok -> {true, unlock(Key, Cas, State)};
        {error, _} = E -> {false, E}
    end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call({mstore, Op, KeyValues, TranscoderOpts, Exp, Cas}, _From, State) ->
    {Connected, Reply} = case connect(State) of
        ok -> {true, mstore(Op, KeyValues, TranscoderOpts, Exp, Cas, State)};
        {error, _} = E -> {false, E}
    end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call({mget, Keys, Exp, Lock}, _From, State) ->
    {Connected, Reply} = case connect(State) of
        ok -> {true, mget(Keys, Exp, Lock, State)};
        {error, _} = E -> {false, E}
    end,
    {reply, Reply, State#instance{connected = Connected}};

handle_call({mget, Keys, Exp, Lock, Type}, _From, State) ->
    {Connected, Reply} = case connect(State) of
        ok -> {true, mget(Keys, Exp, Lock, Type, State)};
        {error, _} = E -> {false, E}
    end,
    {reply, Reply, State#instance{connected = Connected}};

handle_call({arithmetic, Key, OffSet, Exp, Create, Initial}, _From, State) ->
    {Connected, Reply} = case connect(State) of
        ok -> {true, arithmetic(Key, OffSet, Exp, Create, Initial, State)};
        {error, _} = E -> {false, E}
    end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call({remove, Key, N}, _From, State) ->
    {Connected, Reply} = case connect(State) of
        ok -> {true, remove(Key, N, State)};
        {error, _} = E -> {false, E}
    end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call({http, Path, Body, ContentType, Method, Chunked}, _From, State) ->
    {Connected, Reply} = case connect(State) of
        ok -> {true, http(Path, Body, ContentType, Method, Chunked, State)};
        {error, _} = E -> {false, E}
    end,
    {reply, Reply, State#instance{connected = Connected}};
handle_call(bucketname, _From, State = #instance{bucketname = BucketName}) ->
    {reply, {ok, BucketName}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
handle_cast({arithmetic, Key, OffSet, Exp, Create, Initial}, State) ->
    {Connected, _Reply} = case connect(State) of
                             ok -> {true, arithmetic(Key, OffSet, Exp, Create, Initial, State)};
                             {error, _} = E -> {false, E}
                         end,
    {noreply, State#instance{connected = Connected}};

handle_cast({worker_proxy_ping, From, Msgs}, State) ->
    gen_server:cast(From, {worker_proxy_pong, self(), Msgs}),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

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
handle_info(_Info, State) ->
    {noreply, State}.

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
terminate(_Reason, _State = #instance{handle = Handle}) ->
    cberl_nif:destroy(Handle),
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

connect(#instance{connected = true}) ->
    ok;
connect(#instance{connected = false, handle = Handle, opts = Opts}) ->
    ok = cberl_nif:control(Handle, op(connect), Opts),
    receive
        ok -> ok;
        {error, _} = E -> E
    end.

mtouch(Keys, ExpTimesE, #instance{handle = Handle}) ->
    ok = cberl_nif:control(Handle, op(mtouch), [Keys, ExpTimesE]),
    receive
        Reply -> Reply
    end.

unlock(Key, Cas, #instance{handle = Handle}) ->
    cberl_nif:control(Handle, op(unlock), [Key, Cas]),
    receive
        Reply -> Reply
    end.

mstore(Op, KeyValues, TranscoderOpts, Exp, Cas,
      #instance{handle = Handle, transcoder = Transcoder}) ->
    StoreValue =
        case TranscoderOpts of
            none -> KeyValues;
            _ ->
                lists:map(
                    fun ({K, V}) ->
                        {K, Transcoder:encode_value(TranscoderOpts, V)}
                    end, KeyValues)
        end,

    ok = cberl_nif:control(Handle, op(mstore), [operation_value(Op), StoreValue,
                           Transcoder:flag(TranscoderOpts), Exp, Cas]),
    receive
        Reply -> Reply
    end.

mget(Keys, Exp, Lock, #instance{handle = Handle, transcoder = Transcoder}) ->
    ok = cberl_nif:control(Handle, op(mget), [Keys, Exp, Lock]),
    receive
        {error, Error} -> {error, Error};
        {ok, Results} ->
            lists:map(fun(Result) ->
                        case Result of
                            {Cas, Flag, Key, Value} ->
                                DecodedValue = Transcoder:decode_value(Flag, Value),
                                {Key, Cas, DecodedValue};
                            {_Key, {error, _Error}} ->
                                Result
                        end
                end, Results)
    end.

mget(Keys, Exp, Lock, {trans, Flag}, #instance{handle = Handle, transcoder = Transcoder}) ->
    ok = cberl_nif:control(Handle, op(mget), [Keys, Exp, Lock]),
    receive
        {error, Error} -> {error, Error};
        {ok, Results} ->
            lists:map(fun(Result) ->
                        case Result of
                            {Cas, _Flag, Key, Value} ->  %% won't use Flag from couchbase bucket
                                DecodedValue = Transcoder:decode_value(Flag, Value),
                                {Key, Cas, DecodedValue};
                            {_Key, {error, _Error}} ->
                                Result
                        end
                end, Results)
    end;

mget(Keys, Exp, Lock, Type, #instance{handle = Handle, transcoder = _Transcoder}) ->
    ok = cberl_nif:control(Handle, op(mget), [Keys, Exp, Lock, Type]),
    receive
        {error, Error} -> {error, Error};
        {ok, Results} ->
            lists:map(fun(Result) ->
                        case Result of
                            {Cas, _Flag, Key, Value} ->
                                case Type of
                                    ?'CBE_LGET' ->
                                        {Key, Cas, binary_to_uint64_list(Value)};
                                    ?'CBE_LDEQUEUE' ->
                                        {Key, Cas, binary_to_uint64(Value)};
                                    ?'CBE_SGET' ->
                                        {Key, Cas, binary_to_uint64_list(Value)};
                                    ?'CBE_LLEN' ->
                                        {Key, Cas, binary_to_uint64(Value)}
                                end;
                            {_Key, {error, _Error}} ->
                                Result
                        end
                end, Results)
    end.

arithmetic(Key, OffSet, Exp, Create, Initial,
           #instance{handle = Handle, transcoder = Transcoder}) ->
    ok = cberl_nif:control(Handle, op(arithmetic), [Key, OffSet, Exp, Create, Initial]),
    receive
        {error, Error} -> {error, Error};
        {ok, {Cas, Flag, Value}} ->
            DecodedValue = Transcoder:decode_value(Flag, Value),
            {ok, Cas, DecodedValue}
    end.

remove(Key, N, #instance{handle = Handle}) ->
    ok = cberl_nif:control(Handle, op(remove), [Key, N]),
    receive
        Reply -> Reply
    end.

http(Path, Body, ContentType, Method, Chunked, #instance{handle = Handle}) ->
    ok = cberl_nif:control(Handle, op(http), [Path, Body, ContentType, Method, Chunked]),
    receive
        Reply -> Reply
    end.

-spec operation_value(operation_type()) -> integer().
operation_value(add) -> ?'CBE_ADD';
operation_value(replace) -> ?'CBE_REPLACE';
operation_value(set) -> ?'CBE_SET';
operation_value(append) -> ?'CBE_APPEND';
operation_value(prepend) -> ?'CBE_PREPEND';
operation_value(lenqueue) -> ?'CBE_LENQUEUE';
operation_value(lremove) -> ?'CBE_LREMOVE';
operation_value(lenqueue_len) -> ?'CBE_LENQUEUE_LEN';
operation_value(lcut_len) -> ?'CBE_LCUT_LEN';
operation_value(sadd) -> ?'CBE_SADD';
operation_value(sremove) -> ?'CBE_SREMOVE';
operation_value(sismember) -> ?'CBE_SISMEMBER'.


-spec op(atom()) -> integer().
op(connect) -> ?'CMD_CONNECT';
op(mstore) -> ?'CMD_MSTORE';
op(mget) -> ?'CMD_MGET';
op(unlock) -> ?'CMD_UNLOCK';
op(mtouch) -> ?'CMD_MTOUCH';
op(arithmetic) -> ?'CMD_ARITHMETIC';
op(remove) -> ?'CMD_REMOVE';
op(http) -> ?'CMD_HTTP'.

-spec canonical_bucket_name(string()) -> string().
canonical_bucket_name(Name) ->
    case Name of
        [] -> "default";
        BucketName -> BucketName
    end.

-spec binary_to_uint64(binary()) -> integer().
binary_to_uint64(Bin) ->
    case Bin of
        <<U64:64/unsigned-big-integer>> -> U64
    end.

-spec binary_to_uint64_list(binary()) -> list().
binary_to_uint64_list(Bin) ->
    case Bin of
        <<U64:64/unsigned-big-integer>> -> [U64];
        <<U64:64/unsigned-big-integer, Rest/binary>> -> [U64 | binary_to_uint64_list(Rest)]
    end.
