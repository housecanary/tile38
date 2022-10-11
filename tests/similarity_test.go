package tests

import "testing"

func subTestSimilarity(t *testing.T, mc *mockServer) {
	runStep(t, mc, "SIMILARITY", scripts_SIMILARITY_test)
}

func scripts_SIMILARITY_test(mc *mockServer) error {
	script_score_similarity := `
		local EPOCH_TIME = os.time({year = 1970, month = 1, day = 1})
		local NOW = os.time(os.date("*t"))
		local SECONDS_IN_A_DAY = 60 * 60 * 24
		
		local function age_months(days)
			if days > 0 then
				return math.floor((NOW - (EPOCH_TIME + days * SECONDS_IN_A_DAY)) / SECONDS_IN_A_DAY) / 30.0
			end
			
			return 0
		end
		
		local function new_comp(similarity, distance, mls_state_date) 
			return {
				score = {
					default = {
						similarity = similarity
					}
				},
				property = {
					mls_state_date = mls_state_date
				},
				distance = distance
			}
		end

		local function get_adjusted_score(
			mean_dist, std_dist, min_dist, dist_max_penalty, dist,
			mean_age, std_age, min_age, age_max_penaly, age, 
			sim_score
		)
			local cdf_dist = tile38.cdf(dist, min_dist, mean_dist, std_dist)
			local cdt_age = tile38.cdf(age, min_age, mean_age, std_age)

			local adj = sim_score - cdf_dist*dist_max_penalty - cdt_age*age_max_penaly

			if adj < 0 then
				adj = 0
			end

			if adj > 100 then 
				adj = 100
			end

			return adj	
		end
		
		local comps = {
			[1] = new_comp(99, 10, 19249),
			[2] = new_comp(80, 100, 109249),
			[3] = new_comp(77, 200, 29249),
			[4] = new_comp(90, 50, 49249)
		}

		local distances = {}
		local ages = {}
		
		for i, comp in ipairs(comps) do
			distances[#distances + 1] = comp.distance
			ages[#ages + 1] = age_months(comp.property.mls_state_date)
		end

		local mean_dist, std_dist, min_dist, max_dist = tile38.mean_std_min_max(distances)
		local mean_age, std_age, min_age, max_age = tile38.mean_std_min_max(ages)

		for i, comp in ipairs(comps) do 
          comp.score.default.adjusted = get_adjusted_score(
			mean_dist, std_dist, min_dist, 5 * 0.75, comp.distance,
			mean_age, std_age, min_age, 20, age_months(comp.property.mls_state_date), 
			comp.score.default.similarity
		  )
		end

		return {
			comps[1].score.default.adjusted,
			comps[2].score.default.adjusted,
			comps[3].score.default.adjusted,
			comps[4].score.default.adjusted
		}
	
	`
	return mc.DoBatch([][]interface{}{
		{"EVAL", script_score_similarity, 0}, {"[83 78 60 79]"},
	})
}
