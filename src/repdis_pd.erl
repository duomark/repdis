%%%------------------------------------------------------------------------------
%%% @copyright (c) 2014, DuoMark International, Inc.
%%% @author Jay Nelson <jay@duomark.com> [http://duomark.com/]
%%% @reference 2014 Development sponsored by TigerText, Inc. [http://tigertext.com/]
%%% @reference The license is based on the template for Modified BSD from
%%%   <a href="http://opensource.org/licenses/BSD-3-Clause">OSI</a>
%%% @doc
%%%   Redis hash functions implemented with process dictionary as the store.
%%% @since 0.1.0
%%% @end
%%%------------------------------------------------------------------------------
-module(repdis_pd).
-author('Jay Nelson <jay@duomark.com>').

%% External interface
-export([
         hget/2, hmget/2, hget/3, hmget/3,
         hset/3, hmset/2, hset/4, hmset/3,
         hdel/2, hdel/3
        ]).

-define(LOW_DB,   0).
-define(HIGH_DB, 15).
-define(VALID_DB_NUM(__Num), __Num >= ?LOW_DB andalso __Num =< ?HIGH_DB).

%% External functions for testing
-export([sentinel/0]).

-type db_num()    :: ?LOW_DB .. ?HIGH_DB.
-type key()       :: iodata().
-type field()     :: iodata().
-type set_value() :: iodata().
-type get_value() :: binary() | nil.

-type field_value_pair()  :: {field(), set_value()}.
-type field_value_pairs() :: [field_value_pair()].

-export_type([key/0, field/0, set_value/0, get_value/0, field_value_pairs/0]).


%%%------------------------------------------------------------------------------
%%% External API
%%%------------------------------------------------------------------------------

-spec hget  (key(),  field()  ) ->  get_value().
-spec hmget (key(), [field()] ) -> [get_value()].

hget (Key, Field)  -> hget (?LOW_DB, Key, Field).
hmget(Key, Fields) -> hmget(?LOW_DB, Key, Fields).


-spec hget  (db_num(), key(),  field()  ) ->  get_value().
-spec hmget (db_num(), key(), [field()] ) -> [get_value()].
     
hget(Db_Num, Key, Field) when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> nil;
        Dict      -> get_field_value(Field, Dict)
    end.

hmget(Db_Num, Key, Fields) when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> lists:duplicate(length(Fields), nil);
        Dict      -> [get_field_value(Field, Dict) || Field <- Fields]
    end.


-spec hset  (key(), field(), set_value() ) -> 0 | 1.
-spec hmset (key(), field_value_pairs()  ) -> ok.

hset (Key, Field, Value)      -> hset (?LOW_DB, Key, Field, Value).
hmset(Key, Field_Value_Pairs) -> hmset(?LOW_DB, Key, Field_Value_Pairs).


-spec hset  (db_num(), key(), field(), set_value() ) -> 0 | 1.
-spec hmset (db_num(), key(), field_value_pairs()  ) -> ok.

hset(Db_Num, Key, Field, Value) when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> set_value_in_new_dict     (Db_Num, Bin_Key, Field, Value);
        Dict      -> set_value_in_existing_dict(Db_Num, Bin_Key, Field, Value, Dict)
    end.

hmset(Db_Num, Key, Field_Value_Pairs) when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> set_values_in_new_dict     (Db_Num, Bin_Key, Field_Value_Pairs);
        Dict      -> set_values_in_existing_dict(Db_Num, Bin_Key, Field_Value_Pairs, Dict)
    end.


-spec hdel(key(), [field()])           -> non_neg_integer().
-spec hdel(db_num(), key(), [field()]) -> non_neg_integer().
    
hdel(Key, Fields) -> hdel(?LOW_DB, Key, Fields).

hdel(Db_Num, Key, Fields) when ?VALID_DB_NUM(Db_Num) ->
    Bin_Key = iolist_to_binary(Key),
    case get_dict(Db_Num, Bin_Key) of
        undefined -> 0;
        Dict      -> Fold_Fn = fun fold_delete_field_value/2,
                     {Num_Deleted, New_Dict} = lists:foldl(Fold_Fn, {0, Dict}, Fields),
                     Dict = put_dict(Db_Num, Bin_Key, New_Dict),
                     Num_Deleted
    end.


%%%------------------------------------------------------------------------------
%%% Internal utilities
%%%------------------------------------------------------------------------------

sentinel() -> <<"$$REPDIS">>.

get_dict(Db_Num, Bin_Key)       -> get(make_pd_key(Db_Num, Bin_Key)).
put_dict(Db_Num, Bin_Key, Dict) -> put(make_pd_key(Db_Num, Bin_Key), Dict).

make_pd_key(Db_Num, Bin_Key) ->
    {sentinel(), Db_Num, Bin_Key}.

get_field_value(Field, Dict) ->
    Bin_Field = iolist_to_binary(Field),
    case dict:find(Bin_Field, Dict) of
        error       -> nil;
        {ok, Value} -> Value
    end.
             
set_value_in_new_dict(Db_Num, Key, Field, Value) ->
    Bin_Key   = iolist_to_binary(Key),
    Bin_Field = iolist_to_binary(Field),
    Bin_Value = iolist_to_binary(Value),
    New_Dict = dict:store(Bin_Field, Bin_Value, dict:new()),
    undefined = put_dict(Db_Num, Bin_Key, New_Dict),
    1.

set_value_in_existing_dict(Db_Num, Key, Field, Value, Dict) ->
    Bin_Key   = iolist_to_binary(Key),
    Bin_Field = iolist_to_binary(Field),
    Bin_Value = iolist_to_binary(Value),
    {Result, New_Dict} = set_field_value(Bin_Field, Bin_Value, Dict),
    Dict = put_dict(Db_Num, Bin_Key, New_Dict),
    Result.

set_field_value(Bin_Field, Bin_Value, Dict) ->
    case {dict:find(Bin_Field, Dict), dict:store(Bin_Field, Bin_Value, Dict)} of
        {error,   Dict2} -> {1, Dict2};
        {{ok, _}, Dict2} -> {0, Dict2}
    end.

set_values_in_new_dict(Db_Num, Key, Field_Value_Pairs) ->
    Bin_Key = iolist_to_binary(Key),
    New_Dict = set_fields(Field_Value_Pairs, dict:new()),
    undefined = put_dict(Db_Num, Bin_Key, New_Dict),
    ok.

set_values_in_existing_dict(Db_Num, Key, Field_Value_Pairs, Dict) ->
    Bin_Key = iolist_to_binary(Key),
    New_Dict = set_fields(Field_Value_Pairs, Dict),
    Dict = put_dict(Db_Num, Bin_Key, New_Dict),
    ok.
    
set_fields(Field_Value_Pairs, Dict) ->
    lists:foldl(fun({Field, Value}, Next_Dict) ->
                        Bin_Field = iolist_to_binary(Field),
                        Bin_Value = iolist_to_binary(Value),
                        dict:store(Bin_Field, Bin_Value, Next_Dict) end,
                Dict, Field_Value_Pairs).

fold_delete_field_value(Field, {Num_Deleted, Curr_Dict} = Curr_Result) ->
    Bin_Field = iolist_to_binary(Field),
    case dict:find(Bin_Field, Curr_Dict) of
        error   -> Curr_Result;
        {ok, _} -> {Num_Deleted + 1, dict:erase(Bin_Field, Curr_Dict)}
    end.
