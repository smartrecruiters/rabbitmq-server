%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ff_controller).
-behaviour(gen_statem).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([enable/1,
         disable/1,
         sync_cluster/0]).

%% Internal use only.
-export([start_link/0,
         run_migration_fun_locally/2]).

%% gen_statem callbacks.
-export([callback_mode/0,
         init/1,
         terminate/3,
         code_change/4,

         standing_by/3,
         waiting_for_end_of_controller_task/3,
         updating_feature_flag_states/3]).

-record(?MODULE, {from,
                  notify = #{}}).

-define(LOCAL_NAME, ?MODULE).
-define(GLOBAL_NAME, {?MODULE, global}).

-define(FF_STATE_CHANGE_LOCK, {feature_flags_state_change, self()}).

%% Default timeout for operations on remote nodes.
-define(TIMEOUT, 60000).

start_link() ->
    gen_statem:start_link({local, ?LOCAL_NAME}, ?MODULE, none, []).

enable(FeatureName) when is_atom(FeatureName) ->
    enable([FeatureName]);
enable(FeatureNames) when is_list(FeatureNames) ->
    ?LOG_DEBUG(
       "Feature flags: REQUEST TO ENABLE: ~p",
       [FeatureNames],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:call(?LOCAL_NAME, {enable, FeatureNames}).

disable(FeatureName) when is_atom(FeatureName) ->
    disable([FeatureName]);
disable(FeatureNames) when is_list(FeatureNames) ->
    {error, unsupported}.

sync_cluster() ->
    ?LOG_DEBUG(
       "Feature flags: SYNCING FEATURE FLAGS in cluster...",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:call(?LOCAL_NAME, sync_cluster).

%% --------------------------------------------------------------------
%% gen_statem callbacks.
%% --------------------------------------------------------------------

callback_mode() ->
    state_functions.

init(_Args) ->
    {ok, standing_by, none}.

standing_by(
  {call, From} = EventType, EventContent, none)
  when EventContent =/= notify_when_done ->
    case EventContent of
        {enable, FeatureNames} ->
            ?LOG_NOTICE(
               "Feature flags: attempt to enable the following feature "
               "flags: ~p",
               [FeatureNames],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS});
        sync_cluster ->
            ?LOG_NOTICE(
               "Feature flags: attempt to synchronize feature flag states "
               "among running cluster members",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS})
    end,

    %% The first step is to register this process globally (it is already
    %% registered locally). The purpose is to make sure this one takes full
    %% control on feature flag changes among other controllers.
    %%
    %% This is useful for situations where a new node joins the cluster while
    %% a feature flag is being enabled. In this case, when that new node joins
    %% and its controller wants to synchronize feature flags, it will block
    %% and wait for this one to finish.
    case register_globally() of
        yes ->
            %% We would register the process globally. Therefore we can
            %% proceed with enabling/syncing feature flags.
            ?LOG_DEBUG(
               "Feature flags: controller globally registered; can proceed "
               "with task",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            Data = #?MODULE{from = From},
            {next_state, updating_feature_flag_states, Data,
             [{next_event, internal, EventContent}]};

        no ->
            %% Another controller is globally registered. We ask that global
            %% controller to notify us when it is done, and we wait for its
            %% response.
            ?LOG_DEBUG(
               "Feature flags: controller NOT globally registered; need to "
               "wait for the current global controller's task to finish",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            RequestId = notify_me_when_done(),
            {next_state, waiting_for_end_of_controller_task, RequestId,
             [{next_event, EventType, EventContent}]}
    end;
standing_by(
  {call, From}, notify_when_done, none) ->
    %% This state is entered when a globally-registered controller finished
    %% its task but had unhandled `notify_when_done` requests in its inbox. We
    %% just need to notify the caller that it can proceed.
    notify_waiting_controller(From),
    {keep_state_and_data, []}.

waiting_for_end_of_controller_task(
  {call, _From}, _EventContent, _RequestId) ->
    {keep_state_and_data, [postpone]};
waiting_for_end_of_controller_task(
  info, Msg, RequestId) ->
    case gen_statem:check_response(Msg, RequestId) of
        {reply, done} ->
            ?LOG_DEBUG(
               "Feature flags: current global controller's task finished; "
               "trying to take next turn",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {next_state, standing_by, none, []};
        {error, Reason} ->
            ?LOG_DEBUG(
               "Feature flags: error while waiting for current global "
               "controller's task: ~0p; trying to take next turn",
               [Reason],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {next_state, standing_by, none, []};
        no_reply ->
            ?LOG_DEBUG(
               "Feature flags: unknown message while waiting for current "
               "global controller's task: ~0p; still waiting",
               [Msg],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            keep_state_and_data
    end.

updating_feature_flag_states(
  internal, Task, #?MODULE{from = From} = Data) ->
    Reply = proceed_with_task(Task),
    unregister_globally(),
    notify_waiting_controllers(Data),
    {next_state, standing_by, none, [{reply, From, Reply}]};
updating_feature_flag_states(
  {call, From}, notify_when_done, #?MODULE{notify = Notify} = Data) ->
    Notify1 = Notify#{From => true},
    Data1 = Data#?MODULE{notify = Notify1},
    {keep_state, Data1}.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, OldState, OldData, _Extra) ->
    {ok, OldState, OldData}.

%% --------------------------------------------------------------------
%% Code to enable and sync feature flags.
%% --------------------------------------------------------------------

proceed_with_task({enable, FeatureNames}) ->
    enable_task(FeatureNames);
proceed_with_task(sync_cluster) ->
    sync_cluster_task().

enable_task(FeatureNames) ->
    %% We take a snapshot of clustered running nodes at the beginning of the
    %% process and use that list at all time.
    %%
    %% If another (already clustered) node starts meanwhile, it will block in
    %% its startup phase because it will want to synchronize its feature flags
    %% state with the cluster. For that to happen, this controller task needs
    %% to finish before the synchronization task starts.
    %%
    %% For unclustered nodes trying to join the cluster, they will also block
    %% the for same reason.
    %%
    %% If a node stops during the task, this will trigger an RPC error at some
    %% point and the task will abort. That's ok because migration functions
    %% are supposed to be idempotent.
    Nodes = running_nodes(),
    ?LOG_DEBUG(
       "Feature flags: nodes where the feature flags will be enabled: ~p~n"
       "Feature flags: stopped nodes and new nodes joining the cluster in "
       "between will synchronize their feature flag states later.",
       [Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

    %% Likewise, we take a snapshot of the feature flags states on all running
    %% nodes right from the beginning. This is what we use during the entire
    %% task to determine if feature flags are supported, enabled somewhere,
    %% etc.
    case collect_inventory_on_nodes(Nodes) of
        {ok, Inventory} -> enable_many(Inventory, FeatureNames);
        Error           -> Error
    end.

sync_cluster_task() ->
    %% We assume that a feature flag can only be enabled, not disabled.
    %% Therefore this synchronization searches for feature flags enabled on
    %% some nodes, but not all and make sure they are enabled everywhere.
    %%
    %% This happens when a node joins a cluster and that node has a different
    %% set of enabled feature flags. This happens when an already clustered
    %% node starts again, in case a feature flag has been enabled while it was
    %% stopped.
    Nodes = running_nodes(),
    ?LOG_DEBUG(
       "Feature flags: synchronizing feature flags on nodes: ~p",
       [Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

    case collect_inventory_on_nodes(Nodes) of
        {ok, Inventory} ->
            FeatureNames = list_feature_flags_enabled_somewhere(Inventory),
            enable_many(Inventory, FeatureNames);
        Error ->
            Error
    end.

enable_many(Inventory, [FeatureName | Rest]) ->
    case enable_if_supported(Inventory, FeatureName) of
        ok    -> enable_many(Inventory, Rest);
        Error -> Error
    end;
enable_many(_Inventory, []) ->
    ok.

enable_if_supported(Inventory, FeatureName) ->
    case is_supported(Inventory, FeatureName) of
        true ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: supported; continueing",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            case lock_registry_and_enable(Inventory, FeatureName) of
                {ok, _Inventory} -> ok;
                Error            -> Error
            end;
        false ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: unsupported; aborting",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, unsupported}
    end.

lock_registry_and_enable(Inventory, FeatureName) ->
    %% We acquire a lock before make any change to the registry. This is not
    %% used by the controller (because it is already using a globally
    %% registered name to prevent concurrent runs). But this is used in
    %% `rabbit_feature_flags:is_enabled()' to block while the state is
    %% `state_changing'.
    ?LOG_DEBUG(
       "Feature flags: acquiring registry state change lock",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    global:set_lock(?FF_STATE_CHANGE_LOCK),
    Ret = enable_with_registry_locked(Inventory, FeatureName),
    ?LOG_DEBUG(
       "Feature flags: releasing registry state change lock",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    global:del_lock(?FF_STATE_CHANGE_LOCK),
    Ret.

enable_with_registry_locked(Inventory, FeatureName) ->
    %% We verify if the feature flag needs to be enabled somewhere. This may
    %% have changed since the beginning, not because another controller
    %% enabled it (this is not possible because only one can run at a given
    %% time), but because this feature flag was already enabled as a
    %% consequence (for instance, it's a dependency of another feature flag we
    %% processed).
    Nodes = list_nodes_where_feature_flag_is_disabled(Inventory, FeatureName),
    case Nodes of
        [] ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: already enabled; skipping",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {ok, Inventory};
        _ ->
            enable_where_disabled(Inventory, FeatureName)
    end.

enable_where_disabled(Inventory, FeatureName) ->
    %% The feature flag is marked as `state_changing' on all running nodes who
    %% know it, including those where it's already enabled. The idea is that
    %% the feature flag is between two states on some nodes and the code
    %% running on the nodes where the feature flag enabled shouldn't assume
    %% all nodes in the cluster are in the same situation.
    Nodes = list_nodes_who_know_the_feature_flag(Inventory, FeatureName),
    Ret1 = mark_as_enabled_on_nodes(
             Nodes, Inventory, FeatureName, state_changing),
    case Ret1 of
        %% We ignore the returned updated inventory because we don't need or
        %% even want to remember the `state_changing' state. This is only used
        %% for external queries of the registry.
        {ok, _Inventory} ->
            case do_enable(Inventory, FeatureName) of
                {ok, Inventory1} ->
                    mark_as_enabled_on_nodes(
                      Nodes, Inventory1, FeatureName, true);
                Error ->
                    _ = mark_as_enabled_on_nodes(
                          Nodes, Inventory, FeatureName, false),
                    Error
            end;
        Error ->
            _ = mark_as_enabled_on_nodes(
                  Nodes, Inventory, FeatureName, false),
            Error
    end.

do_enable(Inventory, FeatureName) ->
    %% After dependencies are enabled, we need to remember the updated
    %% inventory. This is useful later to skip feature flags enabled earlier
    %% in the process. For instance because a feature flag is dependency of
    %% several other feature flags.
    case enable_dependencies(Inventory, FeatureName) of
        {ok, Inventory1} ->
            %% This time, we only consider nodes where the feature flag is
            %% disabled. The migration function is only executed on those
            %% nodes. I.e. we don't run it again where it has already been
            %% executed.
            Nodes = list_nodes_where_feature_flag_is_disabled(
                      Inventory1, FeatureName),
            Ret = run_migration_fun(Nodes, FeatureName, enable, infinity),
            case Ret of
                ok                        -> {ok, Inventory1};
                {error, no_migration_fun} -> {ok, Inventory1};
                Error                     -> Error
            end;
        Error ->
            Error
    end.

%% --------------------------------------------------------------------
%% Global name registration.
%% --------------------------------------------------------------------

register_globally() ->
    global:register_name(?GLOBAL_NAME, self()).

unregister_globally() ->
    _ = global:unregister_name(?GLOBAL_NAME),
    ok.

notify_me_when_done() ->
    gen_statem:send_request({global, ?GLOBAL_NAME}, notify_when_done).

notify_waiting_controllers(#?MODULE{notify = Notify}) ->
    maps:fold(
      fun(From, true, Acc) ->
              notify_waiting_controller(From),
              Acc
      end, ok, Notify).

notify_waiting_controller({ControlerPid, _} = From) ->
    ControlerNode = node(ControlerPid),
    ?LOG_DEBUG(
       "Feature flags: controller's task finished; notify waiting controller "
       "on node ~p",
       [ControlerNode],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:reply(From, done).

%% --------------------------------------------------------------------
%% Cluster relationship and inventory.
%% --------------------------------------------------------------------

running_nodes() ->
    lists:sort(mnesia:system_info(running_db_nodes)).

collect_inventory_on_nodes(Nodes) ->
    ?LOG_DEBUG(
       "Feature flags: collecting inventory on nodes: ~p",
       [Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Inventory0 = #{feature_flags => #{},
                   applications_per_node => #{},
                   states_per_nodes => #{}},
    collect_inventory_on_nodes(Nodes, Inventory0, ?TIMEOUT).

collect_inventory_on_nodes(
  [Node | Rest],
  #{feature_flags := FeatureFlags,
    applications_per_node := ScannedAppsPerNode,
    states_per_nodes := StatesPerNode} = Inventory,
  Timeout) ->
    case rpc_call(Node, rabbit_ff_registry, inventory, [], Timeout) of
        #{feature_flags := FeatureFlags1,
          applications := ScannedApps,
          states := States} ->
            FeatureFlags2 = maps:merge(FeatureFlags, FeatureFlags1),
            ScannedAppsPerNode1 = ScannedAppsPerNode#{Node => ScannedApps},
            StatesPerNode1 = StatesPerNode#{Node => States},
            Inventory1 = Inventory#{
                           feature_flags => FeatureFlags2,
                           applications_per_node => ScannedAppsPerNode1,
                           states_per_nodes => StatesPerNode1},
            collect_inventory_on_nodes(Rest, Inventory1, Timeout);
        Error ->
            Error
    end;
collect_inventory_on_nodes([], Inventory, _Timeout) ->
    {ok, Inventory}.

list_feature_flags_enabled_somewhere(
  #{states_per_nodes := StatesPerNode}) ->
    %% We want to collect feature flags which are enabled on at least one
    %% node.
    MergedStates = maps:fold(
                     fun(_Node, States, Acc1) ->
                             maps:fold(
                               fun
                                   (FeatureName, true, Acc2) ->
                                       Acc2#{FeatureName => true};
                                   (_FeatureName, false, Acc2) ->
                                       Acc2
                               end, Acc1, States)
                     end, #{}, StatesPerNode),
    lists:sort(maps:keys(MergedStates)).

list_nodes_who_know_the_feature_flag(
  #{states_per_nodes := StatesPerNode},
  FeatureName) ->
    lists:sort(
      maps:keys(
        maps:filter(
          fun(_Node, States) ->
                  maps:is_key(FeatureName, States)
          end, StatesPerNode))).

list_nodes_where_feature_flag_is_disabled(
  #{states_per_nodes := StatesPerNode},
  FeatureName) ->
    lists:sort(
      maps:keys(
        maps:filter(
          fun(_Node, States) ->
                  case States of
                      %% The feature flag is known on this node, run the
                      %% migration function only if it is disabled.
                      #{FeatureName := Enabled} -> not Enabled;


                      %% The feature flags is unknown on this node, don't run
                      %% the migration function.
                      _ -> false
                  end
          end, StatesPerNode))).

rpc_call(Node, Module, Function, Args, Timeout) ->
    case rpc:call(Node, Module, Function, Args, Timeout) of
        {badrpc, {'EXIT',
                  {undef,
                   [{rabbit_feature_flags, Function, Args, []}
                    | _]}}} ->
            %% If rabbit_feature_flags:Function() is undefined
            %% on the remote node, we consider it to be a 3.7.x
            %% pre-feature-flags node.
            %%
            %% Theoretically, it could be an older version (3.6.x and
            %% older). But the RabbitMQ version consistency check
            %% (rabbit_misc:version_minor_equivalent/2) called from
            %% rabbit_mnesia:check_rabbit_consistency/2 already blocked
            %% this situation from happening before we reach this point.
            ?LOG_DEBUG(
               "Feature flags: ~s:~s~p unavailable on node `~s`: "
               "assuming it is a RabbitMQ 3.7.x pre-feature-flags node",
               [?MODULE, Function, Args, Node],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, pre_feature_flags_rabbitmq};
        {badrpc, Reason} = Error ->
            ?LOG_ERROR(
               "Feature flags: error while running ~s:~s~p "
               "on node `~s`: ~p",
               [?MODULE, Function, Args, Node, Reason],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, Error};
        Ret ->
            Ret
    end.

%% --------------------------------------------------------------------
%% Feature flag support queries.
%% --------------------------------------------------------------------

is_supported(
  #{feature_flags := FeatureFlags,
    applications_per_node := ScannedAppsPerNode,
    states_per_nodes := StatesPerNode},
  FeatureName)
  when is_map_key(FeatureName, FeatureFlags) ->
    %% A feature flag is considered supported by a node if:
    %%   - the node knows the feature flag, or
    %%   - the node does not have the application providing it.
    %%
    %% Therefore, we first need to look up the application providing this
    %% feature flag.
    #{FeatureName := #{provided_by := App}} = FeatureFlags,

    maps:fold(
      fun
          (Node, FeatureStates, true) ->
              case FeatureStates of
                  #{FeatureName := _} ->
                      %% The node knows about the feature flag.
                      true;
                  _ ->
                      %% The node doesn't know about the feature flag, so does
                      %% it have the application providing it loaded?
                      #{Node := ScannedApps} = ScannedAppsPerNode,
                      not lists:member(App, ScannedApps)
              end;
          (_Node, _FeatureStates, false) ->
              false
      end, true, StatesPerNode);
is_supported(_Inventory, _FeatureName) ->
    %% None of the nodes know about this feature flag at all.
    false.

%% --------------------------------------------------------------------
%% Feature flag state changes.
%% --------------------------------------------------------------------

mark_as_enabled_on_nodes(
  Nodes,
  #{states_per_nodes := StatesPerNode} = Inventory,
  FeatureName, IsEnabled) ->
    Ret = mark_as_enabled_on_nodes(
            Nodes, StatesPerNode, FeatureName, IsEnabled, ?TIMEOUT),
    case Ret of
        {ok, StatesPerNode1} ->
            Inventory1 = Inventory#{states_per_nodes => StatesPerNode1},
            {ok, Inventory1};
        Error ->
            Error
    end.

mark_as_enabled_on_nodes(
  [Node | Rest], StatesPerNode, FeatureName, IsEnabled, Timeout) ->
    ?LOG_DEBUG(
       "Feature flags: ~s: mark as enabled=~p on node ~p",
       [FeatureName, IsEnabled, Node],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case mark_as_enabled_on_node(Node, FeatureName, IsEnabled, Timeout) of
        ok ->
            States = maps:get(Node, StatesPerNode),
            States1 = States#{FeatureName => IsEnabled},
            StatesPerNode1 = StatesPerNode#{Node => States1},
            mark_as_enabled_on_nodes(
              Rest, StatesPerNode1, FeatureName, IsEnabled, Timeout);
        Error ->
            Error
    end;
mark_as_enabled_on_nodes(
  [], StatesPerNode, _FeatureName, _IsEnabled, _Timeout) ->
    {ok, StatesPerNode}.

mark_as_enabled_on_node(Node, FeatureName, IsEnabled, _Timeout)
  when Node =:= node() ->
    rabbit_feature_flags:mark_as_enabled_locally(FeatureName, IsEnabled);
mark_as_enabled_on_node(Node, FeatureName, IsEnabled, Timeout) ->
    rpc_call(
      Node, rabbit_feature_flags, mark_as_enabled_locally,
      [FeatureName, IsEnabled], Timeout).

%% --------------------------------------------------------------------
%% Feature flag dependencies handling.
%% --------------------------------------------------------------------

enable_dependencies(
  #{feature_flags := FeatureFlags} = Inventory, FeatureName) ->
    #{FeatureName := FeatureProps} = FeatureFlags,
    DependsOn = maps:get(depends_on, FeatureProps, []),
    case DependsOn of
        [] ->
            {ok, Inventory};
        _ ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: enable dependencies: ~p",
               [FeatureName, DependsOn],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            enable_dependencies1(Inventory, DependsOn)
    end.

enable_dependencies1(Inventory, [FeatureName | Rest]) ->
    case enable_with_registry_locked(Inventory, FeatureName) of
        {ok, Inventory1} -> enable_dependencies1(Inventory1, Rest);
        Error            -> Error
    end;
enable_dependencies1(Inventory, []) ->
    {ok, Inventory}.

%% --------------------------------------------------------------------
%% Migration function.
%% --------------------------------------------------------------------

run_migration_fun([Node | Rest], FeatureName, Arg, Timeout) ->
    ?LOG_DEBUG(
       "Feature flags: `~s`: run migration function with arg=~p on node ~p",
       [FeatureName, Arg, Node],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case run_migration_fun_on_node(Node, FeatureName, Arg, Timeout) of
        ok    -> run_migration_fun(Rest, FeatureName, Arg, Timeout);
        Error -> Error
    end;
run_migration_fun([], _FeatureName, _Arg, _Timeout) ->
    ok.

run_migration_fun_on_node(Node, FeatureName, Arg, _Timeout)
  when Node =:= node() ->
    run_migration_fun_locally(FeatureName, Arg);
run_migration_fun_on_node(Node, FeatureName, Arg, Timeout) ->
    rpc_call(
      Node, ?MODULE, run_migration_fun_locally, [FeatureName, Arg],
      Timeout).

run_migration_fun_locally(FeatureName, Arg) ->
    FeatureProps = rabbit_ff_registry:get(FeatureName),
    run_migration_fun_locally(FeatureName, FeatureProps, Arg).

run_migration_fun_locally(FeatureName, FeatureProps, Arg) ->
    case maps:get(migration_fun, FeatureProps, none) of
        {MigrationMod, MigrationFun}
          when is_atom(MigrationMod) andalso is_atom(MigrationFun) ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: run migration function ~p with arg: ~p",
               [FeatureName, MigrationFun, Arg],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            try
                erlang:apply(MigrationMod,
                             MigrationFun,
                             [FeatureName, FeatureProps, Arg])
            catch
                _:Reason:Stacktrace ->
                    ?LOG_ERROR(
                       "Feature flags: `~s`: migration function crashed: "
                       "~p~n~p",
                       [FeatureName, Reason, Stacktrace],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    {error, {migration_fun_crash, Reason, Stacktrace}}
            end;
        none ->
            {error, no_migration_fun};
        Invalid ->
            ?LOG_ERROR(
               "Feature flags: `~s`: invalid migration function: ~p",
               [FeatureName, Invalid],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, {invalid_migration_fun, Invalid}}
    end.
