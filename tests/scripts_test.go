package tests

import (
	"fmt"
	"strings"
	"testing"
)

func subTestScripts(t *testing.T, mc *mockServer) {
	runStep(t, mc, "BASIC", scripts_BASIC_test)
	runStep(t, mc, "ATOMIC", scripts_ATOMIC_test)
	runStep(t, mc, "READONLY", scripts_READONLY_test)
	runStep(t, mc, "NONATOMIC", scripts_NONATOMIC_test)
	runStep(t, mc, "ITERATE", scripts_ITERATE_test)
	runStep(t, mc, "MATH", scripts_MATH_test)
	runStep(t, mc, "STATS", scripts_STATSARRAY_test)
}

func scripts_BASIC_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"EVAL", "return 2 + 2", 0}, {"4"},
		{"SCRIPT LOAD", "return 2 + 2"}, {"2dd1b44209ecb49617af05caf0491390a03c1cc4"},
		{"SCRIPT EXISTS", "2dd1b44209ecb49617af05caf0491390a03c1cc4", "no_script"}, {"[1 0]"},
		{"EVALSHA", "2dd1b44209ecb49617af05caf0491390a03c1cc4", "0"}, {"4"},
		{"SCRIPT FLUSH"}, {"OK"},
		{"SCRIPT EXISTS", "2dd1b44209ecb49617af05caf0491390a03c1cc4", "no_script"}, {"[0 0]"},
		{"EVAL", "return KEYS[1] .. ' only'", 1, "key1"}, {"key1 only"},
		{"EVAL", "return KEYS[1] .. ' and ' .. ARGV[1]", 1, "key1", "arg1"}, {"key1 and arg1"},
		{"EVAL", "return ARGV[1] .. ' and ' .. ARGV[2]", 0, "arg1", "arg2"}, {"arg1 and arg2"},
		{"EVAL", "return tile38.sha1hex('asdf')", 0}, {"3da541559918a808c2402bba5012f6c60b27661c"},
		{"EVAL", "return tile38.distance_to(37.7341129, -122.4408378, 37.733, -122.43)", 0}, {"961"},
		{"EVAL", "return tile38.get('mykey', 'myid1')", "0"}, {nil},
		{"EVAL", "return tile38.call('set', KEYS[1], ARGV[1], 'point', 33.1234, -115.1234)", "1", "mykey", "myid1"}, {"OK"},
		{"EVAL", "local obj = tile38.get('mykey', 'myid1').object; return {tostring(obj.x), tostring(obj.y)}", "0"}, {"[-115.1234 33.1234]"},
		{"EVAL", "return tile38.call('set', KEYS[1], ARGV[1], 'string', 'foobar')", "1", "mykey", "myid2"}, {"OK"},
		{"EVAL", "local obj = tile38.get('mykey', 'myid2').object; return tostring(obj)", "0"}, {"foobar"},
	})
}

func scripts_ATOMIC_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"EVAL", "return tile38.call('get', KEYS[1], ARGV[1])", "1", "mykey", "myid"}, {nil},
		{"EVAL", "return tile38.call('set', KEYS[1], ARGV[1], 'point', 33, -115)", "1", "mykey", "myid1"}, {"OK"},
		{"EVAL", "return tile38.call('get', KEYS[1], ARGV[1], ARGV[2])", "1", "mykey", "myid1", "point"}, {"[33 -115]"},
	})
}

func scripts_READONLY_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"EVALRO", "return tile38.call('get', KEYS[1], ARGV[1])", "1", "mykey", "myid"}, {nil},
		{"EVALRO", "return tile38.call('set', KEYS[1], ARGV[1], 'point', 33, -115)", "1", "mykey", "myid1"}, {
			func(v interface{}) (resp, expect interface{}) {
				s := fmt.Sprintf("%v", v)
				if strings.Contains(s, "ERR read only") {
					return v, v
				}
				return v, "A lua stack containing 'ERR read only'"
			},
		},
		{"EVALRO", "return tile38.pcall('set', KEYS[1], ARGV[1], 'point', 33, -115)", "1", "mykey", "myid1"}, {"ERR read only"},
		{"SET", "mykey", "myid1", "POINT", 33, -115}, {"OK"},
		{"EVALRO", "return tile38.call('get', KEYS[1], ARGV[1], ARGV[2])", "1", "mykey", "myid1", "point"}, {"[33 -115]"},
	})
}

func scripts_NONATOMIC_test(mc *mockServer) error {
	return mc.DoBatch([][]interface{}{
		{"EVALNA", "return tile38.call('get', KEYS[1], ARGV[1])", "1", "mykey", "myid"}, {nil},
		{"EVALNA", "return tile38.call('set', KEYS[1], ARGV[1], 'point', 33, -115)", "1", "mykey", "myid1"}, {"OK"},
		{"EVALNA", "return tile38.call('get', KEYS[1], ARGV[1], ARGV[2])", "1", "mykey", "myid1", "point"}, {"[33 -115]"},
	})
}

func scripts_ITERATE_test(mc *mockServer) error {
	script_ids := `
        local result = {}
		local cursor

		local function process(iterator)
			result[#result + 1] = iterator.id
			return false  -- early stop, after the first object
		end

		cursor = tile38.iterate(
			process, 'WITHIN', 'key2', 'ids', 'get', 'mykey', 'poly8')

		return {cursor, result}
	`
	script_obj := `
        local result = {}
		local cursor

		local function process(iterator)
			result[#result + 1] = iterator.object.json
			return true  -- no early stop, go through all objects
		end

		cursor = tile38.iterate(
			process, 'WITHIN', 'key2', 'ids', 'get', 'mykey', 'poly8')

		return {cursor, result}
	`
	script_fields := `
        local result = {}
		local cursor, foo, bar

		local function process(iterator)
			result[#result + 1] = {iterator:read_fields('foo', 'bar')}
			return false  -- early stop, after the first object
		end

		cursor = tile38.iterate(
			process, 'WITHIN', 'key2', 'ids', 'get', 'mykey', 'poly8')

		return {cursor, result}
	`

	script_nearby_ids := `
        local result = {}
		local cursor

		local function process(iterator)
			result[#result + 1] = iterator.id
			return false  -- early stop, after the first object
		end

		cursor = tile38.iterate(
			process, 'NEARBY', 'key2', 'ids', 'point', 37.7335, -122.4412)

		return {cursor, result}
	`

	poly9 := `{"type":"Polygon","coordinates":[[[-122.44037926197052,37.73313523548048],[-122.44017541408539,37.73313523548048],[-122.44017541408539,37.73336857568778],[-122.44037926197052,37.73336857568778],[-122.44037926197052,37.73313523548048]]]}`

	return mc.DoBatch([][]interface{}{
		{"SET", "mykey", "poly8", "OBJECT", `{"type":"Polygon","coordinates":[[[-122.4408378,37.7341129],[-122.4408378,37.733],[-122.44,37.733],[-122.44,37.7341129],[-122.4408378,37.7341129]],[[-122.44060993194579,37.73345766902749],[-122.44044363498686,37.73345766902749],[-122.44044363498686,37.73355524732416],[-122.44060993194579,37.73355524732416],[-122.44060993194579,37.73345766902749]],[[-122.44060724973677,37.7336888869566],[-122.4402102828026,37.7336888869566],[-122.4402102828026,37.7339752567853],[-122.44060724973677,37.7339752567853],[-122.44060724973677,37.7336888869566]]]}`}, {"OK"},
		{"SET", "key2", "poly9", "FIELD", "foo", 1, "FIELD", "bar", 10, "OBJECT", poly9}, {"OK"},
		{"SET", "key2", "poly10", "OBJECT", `{"type":"Polygon","coordinates":[[[-122.44040071964262,37.73359343010089],[-122.4402666091919,37.73359343010089],[-122.4402666091919,37.73373767596864],[-122.44040071964262,37.73373767596864],[-122.44040071964262,37.73359343010089]]]}`}, {"OK"},
		{"SET", "key2", "poly11", "OBJECT", `{"type":"Polygon","coordinates":[[[-122.44040071964262,37.73359343010089],[-122.4402666091919,37.73359343010089],[-122.4402666091919,37.73373767596864],[-122.44040071964262,37.73373767596864],[-122.44040071964262,37.73359343010089]]]}`}, {"OK"},
		{"SET", "key2", "poly12", "OBJECT", `{"type":"Polygon","coordinates":[[[-122.44040071964262,37.73359343010089],[-122.4402666091919,37.73359343010089],[-122.4402666091919,37.73373767596864],[-122.44040071964262,37.73373767596864],[-122.44040071964262,37.73359343010089]]]}`}, {"OK"},

		// Just make sure that we expect WITHIN to pick poly9 in this setup
		{"WITHIN", "key2", "LIMIT", 1, "IDS", "GET", "mykey", "poly8"}, {"[1 [poly9]]"},

		{"EVAL", script_ids, 0}, {"[1 [poly9]]"}, // early stop, cursor = 1
		{"EVAL", script_obj, 0}, {"[0 [" + poly9 + "]]"}, // no early stop, cursor = 0
		{"EVAL", script_fields, 0}, {"[1 [[1 10]]]"}, // early stop, cursor = 1
		{"EVAL", script_nearby_ids, 0}, {"[1 [poly10]]"}, // early stop, cursor = 1
	})

}

func scripts_MATH_test(mc *mockServer) error {
	script_mean_std_min_max := `
		local data = {[1]=99, [2]=88, [3]=77}
		
		local mean, std, min, max

		mean, std, min, max = tile38.mean_std_min_max(data)

		return {mean, std, min, max}
	`

	script_cdf := `
		local mean, std, min

		mean = 99
		std = 8
		min = 77
		
		local cdf

		cdf = tile38.cdf(90, 10, mean, std)

		return {cdf * 100}
	`

	return mc.DoBatch([][]interface{}{
		{"EVAL", script_cdf, 0}, {"[13]"},
		{"EVAL", script_mean_std_min_max, 0}, {"[88 8 77 99]"},
	})
}

func scripts_STATSARRAY_test(mc *mockServer) error {
	script := `
		local data = tile38.new_stats_array()
		data:append(100)
		data:append(110)
		data:append(200)
		data:append(210)

		local min_cdf = data:cdf(data:min())
		local cdf = data:cdf(200)

		return {min_cdf*100, cdf*100}
	`

	return mc.DoBatch([][]interface{}{
		{"EVAL", script, 0}, {"[13 81]"},
	})
}
