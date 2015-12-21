%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. 十二月 2015 2:44 PM
%%%-------------------------------------------------------------------
-module(cberl_worker_proxy).
-author("zy").

-behaviour(poolboy_worker).
-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    worker_args,
    worker_pid,
    check_counter, mailbox_len, ping_state, check_ping_interval, check_interval, check_overload_threshold
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link(Args :: term()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

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
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init(Args) ->
    process_flag(trap_exit, true),
    {ok, #state{
        worker_args = Args,
        worker_pid = undefined,
        check_counter = 0, mailbox_len = 0,
        ping_state = undefined,
        check_ping_interval = application:get_env(cberl, worker_proxy_check_ping_interval, 2500),
        check_interval = application:get_env(cberl, worker_proxy_check_interval, 5000),
        check_overload_threshold = application:get_env(cberl, worker_proxy_check_overload_threshold, 10000)
    }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
        State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(get_worker, _From, State=#state{worker_pid = undefined}) ->
    case cberl_worker:start_link(State#state.worker_args) of
        {ok, Pid} ->
            {IsOverload, NewState} = overload_threshold_check(Pid, State),
            Reply = case IsOverload of
                        true ->
                            {error, overload};
                        false ->
                            {ok, Pid}
                    end,
            {reply, Reply, NewState#state{worker_pid = Pid}};
        {error, Error} ->
            {reply, {error, Error}, State}
    end;

handle_call(get_worker, _From, State=#state{worker_pid = Pid}) ->
    {IsOverload, NewState} = overload_threshold_check(Pid, State),
    Reply = case IsOverload of
                true ->
                    {error, overload};
                false ->
                    {ok, Pid}
            end,
    {reply, Reply, NewState};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast({worker_proxy_pong, Pid, Msgs}, State=#state{
    worker_pid = WorkerPid,
    ping_state = PingState,
    mailbox_len = Mailbox
}) ->
    ValidReply = (Pid =:= WorkerPid) and (PingState =:= sent),
    NewState = case ValidReply of
                   true ->
                       State#state{
                           mailbox_len = Mailbox - Msgs - 1,
                           ping_state = undefined,
                           check_counter = 0
                       };
                   _ ->
                       State#state{
                           ping_state = undefined
                       }
               end,
    {noreply, NewState};

handle_cast(_Request, State) ->
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
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info({'EXIT', Pid, _}, State=#state{worker_pid = Pid}) ->
    {noreply, State#state{worker_pid = undefined}};

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
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
        State :: #state{}) -> term()).
terminate(_Reason, #state{worker_pid = undefined}) ->
    ok;
terminate(_Reason, #state{worker_pid = Pid}) ->
    cberl_worker:stop(Pid).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
        Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
overload_threshold_check(WorkerPid, State = #state{
    check_counter = Counter, mailbox_len = Mailbox,
    ping_state = PingState,
    check_ping_interval = PingInterval, check_interval = Interval, check_overload_threshold = OverloadThreshold
}) ->
    Counter2 = Counter + 1,
    {NewCounter, NewPingState, MailboxLen} = case Counter2 of
                                                 PingInterval ->
                                                     %% Ping the replica in hopes that we get a pong back before hitting
                                                     %% the hard query interval and triggering an expensive process_info
                                                     %% call. A successful pong from the replica means that all messages
                                                     %% sent before the ping have already been handled and therefore
                                                     %% we can adjust our mailbox estimate accordingly.
                                                     case PingState of
                                                         undefined ->
                                                             gen_server:cast(WorkerPid, {worker_proxy_ping, self(), Mailbox + 1}),
                                                             Mailbox2 = Mailbox + 2,
                                                             PingState2 = sent;
                                                         _ ->
                                                             Mailbox2 = Mailbox + 1,
                                                             PingState2 = PingState
                                                     end,
                                                     {Counter2, PingState2, Mailbox2};
                                                 Interval ->
                                                     %% Time to directly check the mailbox size. This operation may
                                                     %% be extremely expensive. If the replica is currently active,
                                                     %% the proxy will be descheduled until the replica finishes
                                                     %% execution and becomes descheduled itself.
                                                     case erlang:process_info(WorkerPid, message_queue_len) of
                                                         undefined ->
                                                             {0, undefined, OverloadThreshold};
                                                         {_, L} ->
                                                             PingState2 = case PingState of
                                                                              sent ->
                                                                                  %% Ignore pending ping response as it is
                                                                                  %% no longer valid nor useful.
                                                                                  ignore;
                                                                              _ ->
                                                                                  PingState
                                                                          end,
                                                             {0, PingState2, L}
                                                     end;
                                                 _ ->
                                                     {Counter2, PingState, Mailbox + 1}
                                             end,

    {MailboxLen >= OverloadThreshold, State#state{check_counter = NewCounter, mailbox_len = MailboxLen, ping_state = NewPingState}}.