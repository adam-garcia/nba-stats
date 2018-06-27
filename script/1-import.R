#   ____________________________________________________________________________
#   1-import_tidy.R                                                         ####
#   purpose: scrape stats.nba.com for player bio and game data
#   dependencies: libraries in ./0-main.R


##  ............................................................................
##  Aggregate roster data                                                   ####
# From Github:
# https://github.com/seemethere/nba_py/wiki/stats.nba.com-Endpoint-Documentation
nba_teams <- tribble(
  ~"team_name", ~"team_id",
  "Atlanta Hawks", 1610612737,
  "Boston Celtics", 1610612738,
  "Brooklyn Nets", 1610612751,
  "Charlotte Hornets", 1610612766,
  "Chicago Bulls", 1610612741,
  "Cleveland Cavaliers", 1610612739,
  "Dallas Mavericks", 1610612742,
  "Denver Nuggets", 1610612743,
  "Detroit Pistons", 1610612765,
  "Golden State Warriors", 1610612744,
  "Houston Rockets", 1610612745,
  "Indiana Pacers", 1610612754,
  "Los Angeles Clippers", 1610612746,
  "Los Angeles Lakers", 1610612747,
  "Memphis Grizzlies", 1610612763,
  "Miami Heat", 1610612748,
  "Milwaukee Bucks", 1610612749,
  "Minnesota Timberwolves", 1610612750,
  "New Orleans Pelicans", 1610612740,
  "New York Knicks", 1610612752,
  "Oklahoma City Thunder", 1610612760,
  "Orlando Magic", 1610612753,
  "Philadelphia 76ers", 1610612755,
  "Phoenix Suns", 1610612756,
  "Portland Trail Blazers", 1610612757,
  "Sacramento Kings", 1610612758,
  "San Antonio Spurs", 1610612759,
  "Toronto Raptors", 1610612761,
  "Utah Jazz", 1610612762,
  "Washington Wizards", 1610612764
)

# Create season vector in NBA format (e.g., 2008-09)
seasons <- 1984:2017 %>% 
  map_chr(function(y){
    y1 <- (y)
    y2 <- str_sub((y + 1), 3, 4)
    glue("{y1}-{y2}")
  })

# Pull season years (e.g, 2008) from NBA format 
names(seasons) <- map_chr(seasons, function(s){
  str_sub(s, 1, 4)
})


# Download roster data
dir.create("data/in")
nba_teams %>%  
  pull(team_id) %>% 
  walk(function(team){
    # For each team, create a subdirectory in the data/ folder
    new_team <- glue("data/in/{team}")
    if(!dir.exists(new_team)) dir.create(new_team)
    # Map through seasons and pull rosters for each team-season
    walk(seasons, function(s){
      new_season <- glue("data/in/{team}/{s}")
      if (!dir.exists(new_season)) dir.create(new_season)
      # Compose the request
      URL <- glue("http://stats.nba.com/stats/commonteamroster/?Season={s}&TeamID={team}")
      # Make the request, download team data, and close the connection
      h <- new_handle()
      curl_download(URL, glue("data/in/{team}/{s}/_roster.json"), handle = h)
      rm(h)
      gc()
      # Slow requests so as not to overburden
      Sys.sleep(0.15) 
    })
  })

# Parsing JSON data and aggregating into data frame
nba_rosters_in <- nba_teams %>%
  pull(team_id) %>% 
  map(function(team){
    map(seasons, function(s){
      roster_resp <- glue("data/in/{team}/{s}/_roster.json") %>% 
        fromJSON()
      .data <- roster_resp$resultSet$rowSet[[1]] %>%
        as_tibble()
      return(.data)
    }) %>% 
      bind_rows() %>%
      return()
  }) %>% 
  bind_rows() # Some team-years differ in column specification

names(nba_rosters_in) <- list.dirs("data/in") %>% 
  .[2] %>% 
  list.dirs() %>% 
  .[2] %>% 
  glue("/_roster.json") %>% 
  fromJSON() %>% 
  .$resultSets %>% 
  .$headers %>% 
  .[[1]] %>% 
  str_to_lower()

# Data cleaning
nba_rosters <- nba_rosters_in %>% 
  separate(height, c("feet", "inches"), sep = "-", convert = T) %>% 
  mutate(
    num = as.integer(num),
    weight = as.numeric(weight),
    birth_date = str_to_title(birth_date) %>% 
      as.Date("%b %d, %Y"),
    age = as.numeric(age),
    exp = as.numeric(exp) %>% 
      ifelse(is.na(.), 0, .),
    height = feet + inches / 12
  ) %>% 
  select(teamid, season, player_id, player, birth_date, height, everything(), -feet, -inches) %>% 
  distinct(player_id, season, .keep_all = T) %>% 
  group_by(teamid, season) %>%
  mutate(team_players = n())

  
##  ............................................................................
##  Aggregate box score data by player                                      ####
# Pulling box score data
pwalk(list(nba_rosters$season, 
           nba_rosters$teamid,
           nba_rosters$player_id),
      function(season, team, player){
        # Compose request url
        URL <- glue("http://stats.nba.com/stats/playergamelog/",
                    "?PlayerID={player}&",
                    "Season={seasons[season]}&",
                    "SeasonType=Regular%20Season")
        # print(URL)
        # Make the request, download game data, and close the connection
        h <- new_handle()
        curl_download(URL, glue("data/in/{team}/{seasons[season]}/{player}.json"), handle = h)
        rm(h)
        gc()
        # Slow requests so as not to overburden
        Sys.sleep(0.15)
      })


# Assembling by-player box scores into a single data frame
player_games_in <-
  pmap_df(list(nba_rosters$season, 
               nba_rosters$teamid,
               nba_rosters$player_id),
          function(season, team, player){
            game_resp <- glue("data/in/{team}/{seasons[season]}/{player}.json") %>% 
              fromJSON()
            game_resp$resultSets$rowSet[[1]] %>% 
              as_tibble() %>% 
              return()
          })

# Pull column names from a response
names(player_games_in) <- list.dirs("data/in") %>% 
  .[2] %>%
  list.dirs() %>% 
  .[2] %>% 
  list.files(full.names = T) %>% 
  .[2] %>%
  fromJSON() %>% 
  .$resultSets %>% 
  .$headers %>% 
  .[[1]] %>% 
  str_to_lower()

# Clean player game data
player_games <- player_games_in %>% 
  # Convert block of numeric variables
  mutate_if(
    c(rep(F, 6), rep(T, 20), F),
    as.numeric
  ) %>% 
  # Parse out some nested information/dates
  mutate(game_date = game_date %>% 
           str_to_title() %>% 
           as.Date("%b %d, %Y"),
         home_game = str_detect(matchup, "vs\\."),
         win_game = str_detect(wl, "W"),
         season = str_sub(season_id, 2, 5)) %>% 
  separate(matchup, c("team_abr", "opp_abr"), sep = " \\@ | vs\\. ") %>% 
  select(ends_with("_id"),
         contains("game"),
         ends_with("abr"),
         everything(),
         -wl,
         -video_available) %>% 
  distinct(player_id, game_date, .keep_all = T)



##  ............................................................................
##  Combine roster and game data                                            ####
nba <- player_games %>% 
  left_join(nba_rosters, by = c("player_id", "season")) %>% 
  arrange(season, game_date, player_id) %>% 
  group_by(player_id) %>% 
  mutate(first_game = min(game_date),
         tenure = game_date - first_game,
         in_game = TRUE,
         tot_games = cumsum(in_game),
         season = as.numeric(season),
         pts_36 = pts / min * 36) %>% 
  group_by(player_id, season) %>% 
  mutate(player_game_n = n(),
         med_pts = median(pts_36, na.rm = T)) %>% 
  ungroup() %>% 
  group_by(teamid, season) %>% 
  mutate(team_game_n = n(),
         team_win_pct = sum(win_game) / team_game_n) %>% 
  ungroup() %>%
  select(season_id, player_id, player, team_abr, game_id, game_date, opp_abr, 
         everything()) %>% 
  arrange(season, player_id, teamid, game_date)


##  ............................................................................
##  Write data to disk                                                      ####
write_feather(nba, "data/nba.feather")
