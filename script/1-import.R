#   ____________________________________________________________________________
#   1-import_tidy.R                                                         ####
#   purpose: scrape stats.nba.com for player bio and game data
#   dependencies: libraries in ./0-main.R
#                 seemethere/nba_py on Github:
# https://github.com/seemethere/nba_py/wiki/stats.nba.com-Endpoint-Documentation

##  ............................................................................
##  Download league data                                                    ####
h <- new_handle()
t <- tempfile()
curl_download(
  glue(
    "https://raw.githubusercontent.com/seemethere/nba_py/",
    "d8d6524333a79ca793471eb5c04b1a0c64420add/nba_py/constants.py"
  ),
  handle = h,
  destfile = t
)

nba_teams <- readLines(t) %>% 
  .[9:371] %>% 
  str_replace("TEAMS = ", "") %>%
  str_replace_all("'", '\\"') %>% 
  collapse() %>% 
  fromJSON() %>% 
  map_df(function(t){
    t$color2 <- t$colors[2]
    t$colors <- NULL
    return(t)
  }) %>% 
  rename(teamid = id)
h <- NULL
gc()

##  ............................................................................
##  Aggregate roster data                                                   ####
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
dir.create("data")
dir.create("data/in")
nba_teams %>%  
  pull(teamid) %>% 
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
  pull(teamid) %>% 
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
  left_join(nba_teams, by = "teamid") %>% 
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
# Entire dataset
write_feather(nba, "data/nba.feather")

# Split by season
nba %>% 
  split(.$season) %>% 
  walk(function(s){
    id <- s %>% 
      pull(season) %>% 
      .[1]
    write_csv(s, glue("data/{id}.csv"))
  })
