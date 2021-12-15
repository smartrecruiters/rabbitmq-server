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
         sync_cluster/0,
         are_supported/1,
         are_supported/2]).

%% Internal use only.
-export([start_link/0,
         are_supported_locally/1,
         mark_as_enabled_locally/2,
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
    ?LOG_DEBUG(
       "Feature flags: filter out already enabled feature flags",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    DisabledFeatureNames = [FeatureName
                            || FeatureName <- FeatureNames,
                               %% This check can't block because only one
                               %% controller can modify feature flag states at
                               %% a time.
                               not rabbit_feature_flags:is_enabled(FeatureName)
                           ],
    case DisabledFeatureNames of
        _ when DisabledFeatureNames =/= [] ->
            ?LOG_DEBUG(
               "Feature flags: feature flags left to enable: ~p",
               [DisabledFeatureNames],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            ?LOG_DEBUG(
               "Feature flags: checking running nodes before we enable "
               "feature flags (this requires all of them to run)",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            case want_all_nodes() of
                {ok, Nodes} ->
                    ?LOG_DEBUG(
                       "Feature flags: nodes where the feature flags will "
                       "be enabled: ~p~n"
                       "Feature flags: new nodes joining the cluster in "
                       "between will be taken care of later",
                       [Nodes],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    case enable_if_supported(Nodes, DisabledFeatureNames) of
                        ok ->
                            ?LOG_NOTICE(
                               "Feature flags: following feature flags "
                               "enabled: ~p",
                               [FeatureNames],
                               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                            ok;
                        Error ->
                            ?LOG_ERROR(
                               "Feature flags: error(s) while enabling "
                               "feature flags:~n~p",
                               [FeatureNames, Error],
                               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                            Error
                    end;
                Error ->
                    Error
            end;
        [] ->
            %% All specified feature flags are already enabled, there is
            %% nothing to do.
            ?LOG_DEBUG(
               "Feature flags: all requested feature flags already enabled",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            ok
    end;
proceed_with_task(sync_cluster) ->
    %% We assume that a feature flag can only be enabled, not disabled.
    %% Therefore this synchronization searches for feature flags enabled on
    %% some nodes, but not all and make sure they are enabled everywhere.
    %%
    %% This happens when a node joins a cluster and that node has a different
    %% set of enabled feature flags.
    Nodes = running_nodes(),
    ?LOG_DEBUG(
       "Feature flags: synchronizing feature flags among running nodes: ~p",
       [Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case get_enabled_feature_flags_inventory(Nodes) of
        {ok, Inventory} ->
            DisabledPerNode = organize_inventory_per_node(Nodes, Inventory),
            case maps:size(DisabledPerNode) of
                0 ->
                    ?LOG_NOTICE(
                       "Feature flags: all running nodes are in sync "
                       "feature-flags-wise",
                       [],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    ok;
                _ ->
                    ?LOG_DEBUG(
                       "Feature flags: list of feature flags to enable per "
                       "node: ~p",
                       [DisabledPerNode],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    case sync_on_nodes(DisabledPerNode) of
                        ok ->
                            ?LOG_NOTICE(
                               "Feature flags: following nodes "
                               "synchronized: ~p",
                               [Nodes],
                               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                            ok;
                        Error ->
                            ?LOG_ERROR(
                               "Feature flags: error(s) while synchronizing "
                               "nodes ~0p:~n~p",
                               [Nodes, Error],
                               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                            Error
                    end
            end;
        Error ->
            ?LOG_DEBUG(
               "Feature flags: failed to query enabled feature flags on "
               "running nodes: ~p",
               [Error],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            Error
    end.

enable_if_supported(Nodes, FeatureNames) ->
    ?LOG_DEBUG(
       "Feature flags: checking all requested feature flags are supported",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case are_supported_on_nodes(Nodes, FeatureNames) of
        true  -> enable_on_nodes(Nodes, FeatureNames);
        false -> {error, unsupported}
    end.

enable_on_nodes(Nodes, FeatureNames) ->
    ?LOG_DEBUG(
       "Feature flags: acquiring registry state change lock",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    global:set_lock(?FF_STATE_CHANGE_LOCK),
    Ret = enable_on_nodes_locked(Nodes, FeatureNames),
    ?LOG_DEBUG(
       "Feature flags: releasing registry state change lock",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    global:del_lock(?FF_STATE_CHANGE_LOCK),
    Ret.

enable_on_nodes_locked(Nodes, [FeatureName | Rest]) ->
    Ret = case mark_as_enabled_on_nodes(Nodes, FeatureName, state_changing) of
              ok ->
                  case do_enable_on_nodes_locked(Nodes, FeatureName) of
                      ok ->
                          mark_as_enabled_on_nodes(Nodes, FeatureName, true);
                      Error ->
                          Error
                  end;
              Error ->
                  Error
          end,
    case Ret of
        ok ->
            enable_on_nodes_locked(Nodes, Rest);
        _ ->
            _ = mark_as_enabled_on_nodes(Nodes, FeatureName, false),
            Ret
    end;
enable_on_nodes_locked(_Node, []) ->
    ok.

do_enable_on_nodes_locked(Nodes, FeatureName) ->
    case enable_dependencies_on_nodes(Nodes, FeatureName) of
        ok ->
            Ret = run_migration_fun_on_nodes(
                    Nodes, FeatureName, enable, infinity),
            case Ret of
                ok                        -> ok;
                {error, no_migration_fun} -> ok;
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
%% Cluster relationship.
%% --------------------------------------------------------------------

want_all_nodes() ->
    AllNodes = all_nodes(),
    RunningNodes = running_nodes(),
    case AllNodes -- RunningNodes of
        [] ->
            ?LOG_DEBUG(
               "Feature flags: all nodes in the cluster are running, can "
               "proceed with the task",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {ok, AllNodes};
        StoppedNodes ->
            ?LOG_ERROR(
               "Feature flags: some nodes in the cluster are stopped: ~0p; "
               "abort task",
               [StoppedNodes],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, {missing_nodes, StoppedNodes}}
    end.

all_nodes() ->
    lists:sort(mnesia:system_info(db_nodes)).

running_nodes() ->
    lists:sort(mnesia:system_info(running_db_nodes)).

run_feature_flags_mod_on_remote_node(Node, Module, Function, Args, Timeout) ->
    %% FIXME: Feature flags protocol versioning?
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

-spec are_supported(FeatureNames) -> AreSupported when
    FeatureNames :: [rabbit_feature_flags:feature_name()],
    AreSupported :: boolean().

are_supported(FeatureNames) ->
    are_supported(FeatureNames, ?TIMEOUT).

-spec are_supported(FeatureNames, Timeout) -> AreSupported when
    FeatureNames :: [rabbit_feature_flags:feature_name()],
    Timeout :: timeout(),
    AreSupported :: boolean().

are_supported(FeatureNames, Timeout) ->
    Nodes = running_nodes(),
    are_supported_on_nodes(Nodes, FeatureNames, Timeout).

-spec are_supported_on_nodes(Nodes, FeatureNames) -> AreSupported when
    Nodes :: [node()],
    FeatureNames :: [rabbit_feature_flags:feature_name()],
    AreSupported :: boolean().

are_supported_on_nodes(Nodes, FeatureNames) ->
    are_supported_on_nodes(Nodes, FeatureNames, ?TIMEOUT).

-spec are_supported_on_nodes(Nodes, FeatureNames, Timeout) -> AreSupported when
    Nodes :: [node()],
    FeatureNames :: [rabbit_feature_flags:feature_name()],
    Timeout :: timeout(),
    AreSupported :: boolean().
%% @doc
%% Returns if a set of feature flags is supported by specified remote nodes.
%%
%% @param RemoteNodes The list of remote nodes to query.
%% @param FeatureNames A list of names of the feature flag(s) to be checked.
%% @param Timeout Time in milliseconds after which the RPC gives up.
%% @returns `true' if the set of feature flags is entirely supported by
%%   all nodes, or `false' if one of them is not or the RPC timed out.

are_supported_on_nodes(_, [], _) ->
    ?LOG_DEBUG(
      "Feature flags: skipping query for feature flags support as the "
      "given list is empty",
      [],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    true;
are_supported_on_nodes([Node | Rest], FeatureNames, Timeout) ->
    case are_supported_on_node(Node, FeatureNames, Timeout) of
        true ->
            are_supported_on_nodes(Rest, FeatureNames, Timeout);
        false ->
            ?LOG_DEBUG(
              "Feature flags: stopping query for support for ~p here",
              [FeatureNames],
              #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            false
    end;
are_supported_on_nodes([], FeatureNames, _Timeout) ->
    ?LOG_DEBUG(
      "Feature flags: all nodes support ~p",
      [FeatureNames],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    true.

are_supported_on_node(Node, FeatureNames, Timeout) ->
    ?LOG_DEBUG(
      "Feature flags: querying `~p` support on node ~p...",
      [FeatureNames, Node],
      #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Ret = case node() of
              Node ->
                  are_supported_locally(FeatureNames);
              _ ->
                  run_feature_flags_mod_on_remote_node(
                    Node, ?MODULE, are_supported_locally, [FeatureNames],
                    Timeout)
          end,
    case Ret of
        true ->
            ?LOG_DEBUG(
               "Feature flags: node ~p supports ~p",
               [Node, FeatureNames],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            true;
        false ->
            ?LOG_DEBUG(
               "Feature flags: node ~p does not support ~p",
               [Node, FeatureNames],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            false;
        {error, pre_feature_flags_rabbitmq} ->
            %% See run_feature_flags_mod_on_remote_node/5 for
            %% an explanation why we consider this node a 3.7.x
            %% pre-feature-flags node.
            ?LOG_DEBUG(
               "Feature flags: no feature flags support on node ~p, "
               "consider the feature flags unsupported: ~p",
               [Node, FeatureNames],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            false;
        {error, Reason} ->
            ?LOG_DEBUG(
               "Feature flags: error while querying ~p support on "
               "node ~p: ~p",
               [FeatureNames, Node, Reason],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            false
    end.

-spec are_supported_locally(FeatureNames) -> AreSupported when
      FeatureNames :: FeatureName | [FeatureNames],
      FeatureName :: rabbit_feature_flags:feature_name(),
      AreSupported :: boolean().
%% @doc
%% Returns if a single feature flag or a set of feature flags is
%% supported by the local node.
%%
%% @param FeatureNames The name or a list of names of the feature flag(s)
%%   to be checked.
%% @returns `true' if the set of feature flags is entirely supported, or
%%   `false' if one of them is not.

are_supported_locally(FeatureName) when is_atom(FeatureName) ->
    rabbit_ff_registry:is_supported(FeatureName);
are_supported_locally(FeatureNames) when is_list(FeatureNames) ->
    lists:all(fun(F) -> rabbit_ff_registry:is_supported(F) end, FeatureNames).

%% --------------------------------------------------------------------
%% Feature flag state changes.
%% --------------------------------------------------------------------

mark_as_enabled_on_nodes(Nodes, FeatureName, IsEnabled) ->
    mark_as_enabled_on_nodes(Nodes, FeatureName, IsEnabled, ?TIMEOUT).

mark_as_enabled_on_nodes([Node | Rest], FeatureName, IsEnabled, Timeout) ->
    ?LOG_DEBUG(
       "Feature flags: ~s: mark as enabled=~p on node ~p",
       [FeatureName, IsEnabled, Node],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case mark_as_enabled_on_node(Node, FeatureName, IsEnabled, Timeout) of
        ok ->
            mark_as_enabled_on_nodes(Rest, FeatureName, IsEnabled, Timeout);
        Error ->
            Error
    end;
mark_as_enabled_on_nodes([], _FeatureName, _IsEnabled, _Timeout) ->
    ok.

mark_as_enabled_on_node(Node, FeatureName, IsEnabled, _Timeout)
  when Node =:= node() ->
    mark_as_enabled_locally(FeatureName, IsEnabled);
mark_as_enabled_on_node(Node, FeatureName, IsEnabled, Timeout) ->
    run_feature_flags_mod_on_remote_node(
      Node, ?MODULE, mark_as_enabled_locally, [FeatureName, IsEnabled],
      Timeout).

mark_as_enabled_locally(FeatureName, IsEnabled) ->
    ?LOG_DEBUG(
       "Feature flags: ~s: mark as enabled=~p",
       [FeatureName, IsEnabled],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    EnabledFeatureNames = maps:keys(rabbit_feature_flags:list(enabled)),
    NewEnabledFeatureNames = case IsEnabled of
                                 true ->
                                     [FeatureName | EnabledFeatureNames];
                                 false ->
                                     EnabledFeatureNames -- [FeatureName];
                                 state_changing ->
                                     EnabledFeatureNames
                             end,
    WrittenToDisk = case NewEnabledFeatureNames of
                        EnabledFeatureNames ->
                            rabbit_ff_registry:is_registry_written_to_disk();
                        _ ->
                            ok =:= try_to_write_enabled_feature_flags_list(
                                     NewEnabledFeatureNames)
                    end,
    rabbit_feature_flags:initialize_registry(#{},
                                             #{FeatureName => IsEnabled},
                                             WrittenToDisk).

try_to_read_enabled_feature_flags_list() ->
    File = rabbit_feature_flags:enabled_feature_flags_list_file(),
    case file:consult(File) of
        {ok, [List]} ->
            List;
        {error, enoent} ->
            %% If the file is missing, we consider the list of enabled
            %% feature flags to be empty.
            [];
        {error, Reason} = Error ->
            ?LOG_ERROR(
               "Feature flags: failed to read the `feature_flags` "
               "file at `~ts`: ~s",
               [File, file:format_error(Reason)],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            Error
    end.

try_to_write_enabled_feature_flags_list(FeatureNames) ->
    %% Before writing the new file, we read the existing one. If there
    %% are unknown feature flags in that file, we want to keep their
    %% state, even though they are unsupported at this time. It could be
    %% that a plugin was disabled in the meantime.
    %%
    %% FIXME: Lock this code to fix concurrent read/modify/write.
    PreviouslyEnabled = case try_to_read_enabled_feature_flags_list() of
                            {error, _} -> [];
                            List       -> List
                        end,
    FeatureNames1 = [Name
                     || Name <- PreviouslyEnabled,
                        not are_supported_locally(Name)] ++ FeatureNames,
    FeatureNames2 = lists:sort(FeatureNames1),

    File = rabbit_feature_flags:enabled_feature_flags_list_file(),
    Content = io_lib:format("~p.~n", [FeatureNames2]),
    %% TODO: If we fail to write the the file, we should spawn a process
    %% to retry the operation.
    case file:write_file(File, Content) of
        ok ->
            ok;
        {error, Reason} = Error ->
            ?LOG_ERROR(
               "Feature flags: failed to write the `feature_flags` "
               "file at `~s`: ~s",
               [File, file:format_error(Reason)],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            Error
    end.

%% --------------------------------------------------------------------
%% Feature flag dependencies handling.
%% --------------------------------------------------------------------

-spec enable_dependencies_on_nodes(Nodes, FeatureName) -> Ret when
      Nodes :: [node()],
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: ok | {error, any()} | no_return().
%% @private

enable_dependencies_on_nodes(Nodes, FeatureName) ->
    FeatureProps = rabbit_ff_registry:get(FeatureName),
    DependsOn = maps:get(depends_on, FeatureProps, []),
    case DependsOn of
        [] ->
            ok;
        _ ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: enable dependencies: ~p",
               [FeatureName, DependsOn],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            enable_on_nodes_locked(Nodes, DependsOn)
    end.

%% --------------------------------------------------------------------
%% Migration function.
%% --------------------------------------------------------------------

run_migration_fun_on_nodes([Node | Rest], FeatureName, Arg, Timeout) ->
    ?LOG_DEBUG(
       "Feature flags: `~s`: run migration function with arg=~p on node ~p",
       [FeatureName, Arg, Node],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case run_migration_fun_on_node(Node, FeatureName, Arg, Timeout) of
        ok    -> run_migration_fun_on_nodes(Rest, FeatureName, Arg, Timeout);
        Error -> Error
    end;
run_migration_fun_on_nodes([], _FeatureName, _Arg, _Timeout) ->
    ok.

run_migration_fun_on_node(Node, FeatureName, Arg, _Timeout)
  when Node =:= node() ->
    run_migration_fun_locally(FeatureName, Arg);
run_migration_fun_on_node(Node, FeatureName, Arg, Timeout) ->
    run_feature_flags_mod_on_remote_node(
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

%% --------------------------------------------------------------------
%% Synchronization.
%% --------------------------------------------------------------------

get_enabled_feature_flags_inventory(Nodes) ->
    get_enabled_feature_flags_inventory(Nodes, #{}, ?TIMEOUT).

get_enabled_feature_flags_inventory([Node | Rest], Inventory, Timeout)
  when Node =:= node() ->
    EnabledFeatureFlags = rabbit_feature_flags:list(enabled),
    EnabledFeatureNames = maps:keys(EnabledFeatureFlags),
    Inventory1 = update_inventory(Inventory, Node, EnabledFeatureNames),
    get_enabled_feature_flags_inventory(Rest, Inventory1, Timeout);
get_enabled_feature_flags_inventory([Node | Rest], Inventory, Timeout) ->
    Ret = run_feature_flags_mod_on_remote_node(
            Node, rabbit_feature_flags, list, [enabled], Timeout),
    case Ret of
        EnabledFeatureFlags when is_map(EnabledFeatureFlags) ->
            EnabledFeatureNames = maps:keys(EnabledFeatureFlags),
            Inventory1 = update_inventory(
                           Inventory, Node, EnabledFeatureNames),
            get_enabled_feature_flags_inventory(Rest, Inventory1, Timeout);
        Error ->
            Error
    end;
get_enabled_feature_flags_inventory([], Inventory, _Timeout) ->
    {ok, Inventory}.

update_inventory(Inventory, Node, [FeatureName | Rest]) ->
    Inventory1 = case Inventory of
                     #{FeatureName := Nodes} ->
                         Inventory#{FeatureName => [Node | Nodes]};
                     _ ->
                         Inventory#{FeatureName => [Node]}
                 end,
    update_inventory(Inventory1, Node, Rest);
update_inventory(Inventory, _Node, []) ->
    Inventory.

organize_inventory_per_node(Nodes, Inventory) ->
    EnabledFeatureNames = lists:sort(maps:keys(Inventory)),
    organize_inventory_per_node1(Nodes, EnabledFeatureNames, Inventory, #{}).

organize_inventory_per_node1(
  [Node | Rest], EnabledFeatureNames, Inventory, DisabledPerNode) ->
    DisabledFeatureNames = [FeatureName
                            || FeatureName <- EnabledFeatureNames,
                               not lists:member(
                                     Node,
                                     maps:get(FeatureName, Inventory))],
    case DisabledFeatureNames of
        [] ->
            organize_inventory_per_node1(
              Rest, EnabledFeatureNames, Inventory, DisabledPerNode);
        _ ->
            DisabledPerNode1 = DisabledPerNode#{Node => DisabledFeatureNames},
            organize_inventory_per_node1(
              Rest, EnabledFeatureNames, Inventory, DisabledPerNode1)
    end;
organize_inventory_per_node1(
  [], _EnabledFeatureNames, _Inventory, DisabledPerNode) ->
    DisabledPerNode.

sync_on_nodes(DisabledPerNode) ->
    Nodes = lists:sort(maps:keys(DisabledPerNode)),
    sync_on_nodes(Nodes, DisabledPerNode).

sync_on_nodes([Node | Rest], DisabledPerNode) ->
    #{Node := FeatureNames} = DisabledPerNode,
    ?LOG_DEBUG(
       "Feature flags: feature flags to enable to synchronize node ~p: ~p~n",
       [Node, FeatureNames],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case enable_if_supported([Node], FeatureNames) of
        ok ->
            sync_on_nodes(Rest, DisabledPerNode);
        Error ->
            ?LOG_ERROR(
               "Feature flags: failed to synchronize node ~p: ~p~n",
               [Node, Error],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            Error
    end;
sync_on_nodes([], _DisabledPerNode) ->
    ok.
