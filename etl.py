import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import hashlib
import time
from datetime import datetime, timedelta
import importlib
import sqlalchemy as db
import pytz

# db_params = {
#     "host": "mydatabase.cluster-codg2seac5vh.us-east-1.rds.amazonaws.com",
#     "database": "mydatabase",  # Default database
#     "user": "master_username",
#     "password": "IW08Q5b*uMJd|Hxnl7lnXbE-p!!H",
#     "port": 5432
# }

def main(db_params):
    # URL to scrape
    sportsbet_url = "https://www.sportsbet.com.au/betting/basketball-us/nba"
    print_results = False
    
    # Initialize an empty games and odds table
    games = pd.DataFrame(columns=["GameID", "AwayTeam", "HomeTeam", "StartDate", "StartTime"])
    odds = pd.DataFrame(columns=["GameID", "Date", "Time", "Live","AwayOdds", "HomeOdds"])
    
    # get event containers from sportsbet website where all the data is stored.
    event_containers = get_event_containers_sportsbet(sportsbet_url)
    
    # Loop through each event container
    for event in event_containers:
        # get data
        team_one_name, team_two_name, live, odds_text, dt = extract_match_properties(event, print_results)
        
        # Generate a unique GameID
        gameID = generate_game_id(team_one_name, team_two_name, dt.date())
    
        # put the data into tables.
        games, odds = update_tables(games, odds, gameID, team_one_name, team_two_name, live, odds_text, dt)

    # Load data to AWS
    upload_data_to_AWS(games, odds, db_params)
    print(f"{len(odds)} odds added to database.")



def upload_data_to_AWS(games, odds, db_params):
    # Create the PostgreSQL connection string
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    engine = db.create_engine(db_url)
    
    with engine.connect() as conn:
        # Insert into 'games' with upsert (replace existing rows)
        games_query = """
        INSERT INTO games (gameid, awayteam, hometeam, startdate, starttime)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (gameid) DO UPDATE 
        SET awayteam = EXCLUDED.awayteam,
            hometeam = EXCLUDED.hometeam,
            startdate = EXCLUDED.startdate,
            starttime = EXCLUDED.starttime;
        """
        
        # Insert each row into 'games'
        for _, row in games.iterrows():
            conn.execute(games_query, (row.GameID, row.AwayTeam, row.HomeTeam, row.StartDate, row.StartTime))
    
        # Append 'odds' table (no conflict resolution needed)
        odds.columns = odds.columns.str.lower()  # Ensure column names match PostgreSQL (lowercase)
        odds.to_sql("odds", engine, if_exists="append", index=False)



# convert date and time strings into format that can be stored.
def format_datetime(date_str):
    
    # Add year to the string to make it complete
    date_str = '2025 ' + date_str.split(', ')[1]  # Assuming the year is 2025
    
    # Define the format for parsing the date
    date_format = "%Y %d %b %H:%M"
    
    # Convert to datetime object
    date_obj = datetime.strptime(date_str, date_format)
    
    # Convert to a format that can be stored (e.g., ISO 8601 format)
    timestamp = date_obj.strftime("%Y-%m-%d %H:%M:%S")

    # Convert to datetime object
    dt = pd.to_datetime(timestamp)

    return dt

# extract all the relevant data
def extract_match_properties(event, print_result):
    # Extract team names
    team_one = event.find('div', {'data-automation-id': 'participant-one'})
    team_two = event.find('div', {'data-automation-id': 'participant-two'})
    
    team_one_name = team_one.get_text(strip=True) if team_one else "Team 1 Not Found"
    team_two_name = team_two.get_text(strip=True) if team_two else "Team 2 Not Found"
    
    # Is the game live?
    live_tag = event.find('span', class_='size11_fwt0xu4 bold_f1au7gae badgeText_f1idx9va')   
    if live_tag:
        live = True
    else:
        live = False
    
    # Extract odds
    odds_elements = event.find_all('span', {'class': 'size14_f7opyze bold_f1au7gae priceTextSize_frw9zm9', 'data-automation-id': 'price-text'})
    odds_text = [odd.get_text(strip=True) for odd in odds_elements[:2]]

    # get start time of game
    time_tag = event.find(class_="time_fbgyqei")
    if time_tag:    
        dt = format_datetime(time_tag.string)

    # print results
    if print_result:
        print(f"Match: {team_one_name} @ {team_two_name}")
        print(f"Game is live: {live}")
        print(f"Date: {dt.date()}, Time: {dt.time()}")
        print(f"Odds: {', '.join(odds_text)}")
        print('-' * 40)  # Separator for readability
        

    return team_one_name, team_two_name, live, odds_text, dt

# go to website and extract event containers
def get_event_containers_sportsbet(sportsbet_url):
    # Send a GET request to the website
    response = requests.get(sportsbet_url)
    
    # Parse the HTML content of the page
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find all event containers
    event_containers = soup.find_all('li', {'class': 'cardOuterItem_fn8ai8t'})

    return event_containers

# take teams + data and generate a unique ID.
def generate_game_id(team_one_name, team_two_name, date):
    unique_string = f"{team_one_name}_{team_two_name}_{date}"
    return int(hashlib.md5(unique_string.encode()).hexdigest(), 16) % (10**8)  # 8-digit GameID

# upload the new date in the relevant tables
def update_tables(games, odds, gameID, team_one_name, team_two_name, live, odds_text, dt):

    # Set the correct timezone (Australia/Sydney)
    timezone_aus = pytz.timezone("Australia/Sydney")
    current_time_aus = datetime.now(timezone_aus)  # Current time in Australia/Sydney

    # New game details 
    new_game = {
        "GameID": gameID,
        "AwayTeam": team_one_name,
        "HomeTeam": team_two_name,
        "StartDate": dt.date(),
        "StartTime": dt.time(),
    }

    # if odds are found
    if odds_text:
        # new odds details
        new_odds = {
            "GameID": gameID,
            "Date": datetime.today().date(), 
            "Time": current_time_aus.replace(tzinfo=None),
            "Live": live,
            "AwayOdds": float(odds_text[0]), 
            "HomeOdds": float(odds_text[1])
        }

        # Add row to DataFrame
        if games.empty == True:
            games = pd.DataFrame([new_game])  # Initialize with first row
            odds = pd.DataFrame([new_odds]) 
        else:
            games = pd.concat([games, pd.DataFrame([new_game])], ignore_index=True)
            odds = pd.concat([odds, pd.DataFrame([new_odds])], ignore_index=True)

    return games, odds


# # Within 90minutes of the start time of any game today, change the frequency update to every 5minutes.
# def get_schedule_interval(db_params):

#     # Define the database connection string
#     db_connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    
#     # Create an SQLAlchemy engine
#     engine = db.create_engine(db_connection_string)
    
#     # Step 1: Get all games on a specific date
#     query_games = """
#     SELECT gameid, awayteam, hometeam, startdate, starttime FROM games WHERE startdate = %s;
#     """
    
#     date_filter = datetime.today().date()
#     games = pd.read_sql(query_games, engine, params=(date_filter,))
    
#     # Calculate time difference in minutes
#     time_diff_minutes = games['starttime'].apply(lambda x: (datetime.combine(datetime.today(), x) - datetime.combine(datetime.today(), datetime.now().time())).total_seconds() / 60)
    
#     # Check if games are on today's date and within 90 minutes of current time
#     time_diff_bool = (games['startdate'] == datetime.today().date()) & (abs(time_diff_minutes) < 90)    
    
#     # 5 minute or 30 minute updates
#     if time_diff_bool.any():
#         return timedelta(minutes=5)
#     else:
#         return timedelta(minutes=30)
        