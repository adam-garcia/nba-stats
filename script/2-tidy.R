#   ____________________________________________________________________________
#   2-tidy.R                                                                ####
#   purpose: tidy data in preparation for analysis
#   dependencies: ./1-import.R

# First, load data exported by 1-import.R
nba <- read_feather("data/nba.feather")

# Now we'll construct our longitudinal dataset of players consecutive seasons

debut <- nba %>% 
  distinct(season, player, player_id, exp) %>% 
  arrange(player, season) %>% 
  group_by(player) %>% 
  mutate(has_rookie = any(exp == 0),
         rookie_szn = min(season)) %>% 
  filter(has_rookie) %>% 
  arrange(player, season) %>% 
  mutate(
    consec_yr = seq_along(player) + rookie_szn - 1 == season
    ) %>% 
  filter(consec_yr) %>% 
  add_tally() %>% 
  ungroup() %>% 
  filter(n > 4) %>% 
  select(player_id, season, rookie_szn) %>% 
  left_join(nba) %>% 
  gather(metric, value, min:plus_minus)

# Write to disk
debut %>% 
  split(.$season) %>%
  walk(function(s){
    split(s, s$metric) %>% 
      walk(function(m){
        yr <- m %>% 
          pull(season) %>% 
          .[1]
        mt <- m %>% 
          pull(metric) %>% 
          .[1]
        m %>% 
          write_csv(glue("data/debut/{yr}_{mt}.csv"))
      })
  })
