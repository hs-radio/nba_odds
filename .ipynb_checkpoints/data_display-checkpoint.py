from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
import importlib



# Function to convert a single time string to total seconds
def time_to_seconds(time_str):
    # Check if the time string includes microseconds
    try:
        time_obj = datetime.strptime(str(time_str), "%H:%M:%S.%f").time()
    except ValueError:
        # If no microseconds, use a format that handles just hours, minutes, and seconds
        time_obj = datetime.strptime(str(time_str), "%H:%M:%S").time()
    
    return time_obj.hour * 3600 + time_obj.minute * 60 + time_obj.second + time_obj.microsecond / 1e6


# Function to convert all time values in the DataFrame to total seconds
def df_time_to_seconds_array(game_odds):
    time_values_array = game_odds['time'].values
    # Apply the conversion to each time value in the array
    total_seconds_array = [time_to_seconds(time) for time in time_values_array]
    return total_seconds_array




# plot the line movements on the date 'on_date'
def plot_line_movements(db_params, on_date):
    # Define the database connection string
    db_connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    
    # Create an SQLAlchemy engine
    engine = create_engine(db_connection_string)
    
    # Step 1: Get all games on a specific date
    query_games = """
    SELECT gameid, awayteam, hometeam, starttime FROM games WHERE startdate = %s;
    """
    
    date_filter = on_date.date()
    games_df = pd.read_sql(query_games, engine, params=(date_filter,))
    
    # Step 2: Get odds for the matching games
    game_ids = tuple(games_df["gameid"].tolist())
    if game_ids:  # Only query if there are matching games
        if len(game_ids) == 1:
            # For a single game_id, use '=' instead of IN
            query_odds = f"""
            SELECT gameid, date, time, awayodds, homeodds 
            FROM odds 
            WHERE gameid = {game_ids[0]}
            ORDER BY gameid, time;
            """
        else:
            # For multiple game_ids, use IN
            query_odds = f"""
            SELECT gameid, date, time, awayodds, homeodds 
            FROM odds 
            WHERE gameid IN {game_ids}
            ORDER BY gameid, time;
            """
        odds_df = pd.read_sql(query_odds, engine)
    
        # Step 3: Plot odds over time for each game
        for game_id in game_ids:
            game_odds = odds_df[odds_df["gameid"] == game_id]
            
            # Get away and home team names from the games dataframe
            game_info = games_df[games_df["gameid"] == game_id].iloc[0]
            print(game_info)
            away_team = game_info["awayteam"]
            home_team = game_info["hometeam"]
            starttime_seconds = time_to_seconds(game_info['starttime'])  # Start time of the game in seconds

            # Get the difference in days between the startdate and when the data was collected.
            # print(game_odds["date"])
            # print(game_odds["time"])
            game_odds.loc[:, "date"] = pd.to_datetime(game_odds["date"], errors="coerce")
            day_diff = pd.to_timedelta(pd.Timestamp(on_date.date()) - game_odds["date"])
            days_array = day_diff.dt.days.to_numpy()
            
            # Fix time variable (time from odds)
            time_in_seconds = np.array(df_time_to_seconds_array(game_odds))
            
            # Calculate the difference between the odds' time and the game's start time
            time_diff = time_in_seconds - starttime_seconds  # Difference in seconds
            time_diff_minutes = time_diff / 60 - 1440 * days_array # Convert to minutes
            
            # Create a new plot for each game
            plt.figure(figsize=(8, 4))
            plt.plot(time_diff_minutes, game_odds['awayodds'].values, marker="o", linestyle="-", label=f"{away_team} Away")
            plt.plot(time_diff_minutes, game_odds['homeodds'].values, marker="s", linestyle="--", label=f"{home_team} Home")
    
            plt.xlabel("Time until game starts (minutes)")
            plt.ylabel("Odds")
            plt.title(f"Odds Over Time for {away_team} vs {home_team} on {on_date.strftime('%Y-%m-%d')}")
            plt.legend()
            plt.xticks(rotation=45)
            plt.grid()
            plt.tight_layout()  # Adjust layout to avoid clipping
            plt.show()
    else:
        print(f"No games found for {on_date.strftime('%Y-%m-%d')}.")
