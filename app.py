import streamlit as st
import pandas as pd

#Data source: https://www.kaggle.com/datasets/nico2890/football-10-years-of-epl-with-goal-timing?resource=download

file_1 = 'goal_time_epl.parquet'
file_2 = 'results_epl.parquet'

goal_times = pd.read_parquet(file_1)
results = pd.read_parquet(file_2)

teams = results['HomeTeam'].unique()
teams.sort()

goal_times.rename(columns={'id':'Game_ID'}, inplace=True)
results.rename(columns={'id':'Game_ID'}, inplace=True)

results_2 = results
results_2.loc[results_2['FTHG'] > results_2['FTAG'], 'Result'] = 1
results_2.loc[results_2['FTHG'] < results_2['FTAG'], 'Result'] = 2
results_2.loc[results_2['FTHG'] == results_2['FTAG'], 'Result'] = 0

complete_results = results.merge(goal_times,on='Game_ID').reset_index(drop=True)
penalties = complete_results[complete_results['Penalty'] == 1].reset_index(drop=True)
club_own_goals_df = complete_results.rename(columns={'FTHG':'Goals', 'Team':'Ground'})

@st.cache_data
def most_goals(goal_type="Goals Scored", ground="All"):
    if goal_type == 'Goals Scored':
        if ground == 'All':
            club_goals = pd.concat([
                results[['HomeTeam', 'FTHG']].rename(columns={'HomeTeam':'Team', 'FTHG':'Goals'}),
                results[['AwayTeam', 'FTAG']].rename(columns={'AwayTeam':'Team', 'FTAG':'Goals'})
            ])
        elif ground == 'Home':
            club_goals = results[['HomeTeam', 'FTHG']].rename(columns={'HomeTeam':'Team', 'FTHG':'Goals'})
        else:
            club_goals = results[['AwayTeam', 'FTAG']].rename(columns={'AwayTeam':'Team', 'FTAG':'Goals'})
    else:
        if ground == 'All':
            club_goals = pd.concat([
                results[['HomeTeam', 'FTAG']].rename(columns={'HomeTeam':'Team', 'FTAG':'Goals'}),
                results[['AwayTeam', 'FTHG']].rename(columns={'AwayTeam':'Team', 'FTHG':'Goals'})
            ])
        elif ground == 'Home':
            club_goals = results[['HomeTeam', 'FTAG']].rename(columns={'HomeTeam':'Team', 'FTAG':'Goals'})
        else:
            club_goals = results[['AwayTeam', 'FTHG']].rename(columns={'AwayTeam':'Team', 'FTHG':'Goals'})

    club_goals = club_goals.groupby('Team').sum().sort_values(by='Goals', ascending=False)
    return club_goals

def team_goals(team, ground='All'):
    goals_df = most_goals(ground=ground).reset_index()
    return int(goals_df[goals_df['Team'] == team]['Goals'])

def club_conceded_goals(team, ground='All', goal_type='Goals Conceded'):
    return int(most_goals(goal_type=goal_type, ground=ground).loc[team])

@st.cache_data
def most_wins_draws_loses(result_type='Wins', ground='All', team='All'):
    if team=='All':
        if result_type == 'Wins':
            if ground == 'All':
                result_games = (results[(results['Result'] == 1)].rename(columns={'HomeTeam':'Team'}).groupby('Team').count()['Game_ID'] + results[(results['Result'] == 2)].rename(columns={'AwayTeam':'Team'}).groupby('Team').count()['Game_ID']).sort_values(ascending=False)
            elif ground =='Home':
                result_games = results[(results['Result'] == 1)].groupby('HomeTeam').count()['Game_ID'].sort_values(ascending=False)
            else:
                result_games = results[(results['Result'] == 2)].groupby('AwayTeam').count()['Game_ID'].sort_values(ascending=False)
        elif result_type == "Loses":
            if ground == 'All':
                result_games = (results[(results['Result'] == 1)].rename(columns={'AwayTeam':'Team'}).groupby('Team').count()['Game_ID'] + results[(results['Result'] == 2)].rename(columns={'HomeTeam':'Team'}).groupby('Team').count()['Game_ID']).sort_values(ascending=False)
            elif ground =='Home':
                result_games = results[(results['Result'] == 2)].groupby('HomeTeam').count()['Game_ID'].sort_values(ascending=False)
            else:
                result_games = results[(results['Result'] == 1)].groupby('AwayTeam').count()['Game_ID'].sort_values(ascending=False)
        else:
            if ground == 'All':
                result_games = (results[(results['Result'] == 0)].rename(columns={'HomeTeam':'Team'}).groupby('Team').count()['Game_ID'] + results[(results['Result'] == 0)].rename(columns={'AwayTeam':'Team'}).groupby('Team').count()['Game_ID']).sort_values(ascending=False)
            elif ground == 'Home':
                result_games = results[(results['Result'] == 0)].groupby('HomeTeam').count()['Game_ID'].sort_values(ascending=False)
            else:
                result_games = results[(results['Result'] == 0)].groupby('AwayTeam').count()['Game_ID'].sort_values(ascending=False)
    else:
        if result_type == 'Wins':
            if ground == 'All':
                result_games = (results[(results['Result'] == 1) & (results['HomeTeam'] == team)].count()['Game_ID'] + results[(results['Result'] == 2) & (results['AwayTeam'] == team)].count()['Game_ID'])
            elif ground =='Home':
                result_games = results[(results['Result'] == 1) & (results['HomeTeam'] == team)].count()['Game_ID']
            else:
                result_games = results[(results['Result'] == 2) & (results['AwayTeam'] == team)].count()['Game_ID']
        elif result_type == "Loses":
            if ground == 'All':
                result_games = (results[(results['Result'] == 1) & (results['AwayTeam'] == team)].count()['Game_ID'] + results[(results['Result'] == 2) & (results['HomeTeam'] == team)].count()['Game_ID'])
            elif ground =='Home':
                result_games = results[(results['Result'] == 2) & (results['HomeTeam'] == team)].count()['Game_ID']
            else:
                result_games = results[(results['Result'] == 1) & (results['AwayTeam'] == team)].count()['Game_ID']
        else:
            if ground == 'All':
                result_games = (results[(results['Result'] == 0) & (results['HomeTeam'] == team)].count()['Game_ID'] + results[(results['Result'] == 0) & (results['AwayTeam'] == team)].count()['Game_ID'])
            elif ground == 'Home':
                result_games = results[(results['Result'] == 0) & (results['HomeTeam'] ==  team)].count()['Game_ID']
            else:
                result_games = results[(results['Result'] == 0) & (results['AwayTeam'] ==  team)].count()['Game_ID']
    return result_games

@st.cache_data
def penalty_results(team='All', ground='All', rows=15):

    def penalty_scorer(row):
        if row['Team'] == 'Home':
            return row['HomeTeam']
        else:
            return row['AwayTeam']

    penalties['Penalties Scored'] = penalties.apply(penalty_scorer, axis=1)
    if team == 'All':
        return penalties.groupby('Penalties Scored').count()['Game_ID'].sort_values(ascending=False).head(rows)
    elif ground == 'All':
        return penalties[((penalties['HomeTeam'] == team) & (penalties['Team'] == 'Home')) | ((penalties['AwayTeam'] == team) & (penalties['Team'] == 'Away'))].count()['Game_ID']
    elif ground == 'Home':
        return penalties[((penalties['HomeTeam'] == team) & (penalties['Team'] == 'Home'))].count()['Game_ID']
    elif ground == 'Away':
        return penalties[((penalties['AwayTeam'] == team) & (penalties['Team'] == 'Away'))].count()['Game_ID']
    

@st.cache_data
def penalty_percentage(rows=100):
    goals_df = most_goals()
    pens_df = penalty_results(rows=100)

    goals_df = goals_df.join(pens_df)
    goals_df.rename(columns={'Game_ID':'Penalties'}, inplace=True)
    goals_df['Penalties Percentage'] = goals_df['Penalties']/goals_df['Goals']*100
    goals_df.sort_values(by='Penalties Percentage', ascending=False, inplace=True)
    goals_df['Penalties Percentage'] = goals_df['Penalties Percentage'].map(lambda x: "{:.2f}%".format(x))
    return goals_df[['Penalties Percentage']].head(rows)

@st.cache_data
def best_win_ratio(team, ground='All', min_games = 5):
    club_wins_home = results[(results['HomeTeam'] == team) & (results['FTHG'] > results['FTAG'])].groupby('AwayTeam')['Game_ID'].count()
    club_wins_away = results[(results['AwayTeam'] == team) & (results['FTHG'] < results['FTAG'])].groupby('HomeTeam')['Game_ID'].count()

    club_loses_home = results[(results['HomeTeam'] == team) & (results['FTHG'] < results['FTAG'])].groupby('AwayTeam')['Game_ID'].count()
    club_loses_away = results[(results['AwayTeam'] == team) & (results['FTHG'] > results['FTAG'])].groupby('HomeTeam')['Game_ID'].count()

    club_draws_home = results[(results['HomeTeam'] == team) & (results['FTHG'] == results['FTAG'])].groupby('AwayTeam')['Game_ID'].count()
    club_draws_away = results[(results['AwayTeam'] == team) & (results['FTHG'] == results['FTAG'])].groupby('HomeTeam')['Game_ID'].count()

    if ground == 'All':
        club_wins = club_wins_home.add(club_wins_away, fill_value=0)
        club_loses = club_loses_home.add(club_loses_away, fill_value=0)
        club_draws = club_draws_home.add(club_draws_away, fill_value=0)

        club_win_loss_ratio = pd.concat([club_wins, club_draws, club_loses], axis=1).fillna(0)
        club_win_loss_ratio.columns = ["Wins", "Draws", "Loses"]
        club_win_loss_ratio['Total Games'] = club_win_loss_ratio['Wins'] + club_win_loss_ratio['Draws']  + club_win_loss_ratio['Loses'] 
        club_win_loss_ratio.astype(int)
        club_win_loss_ratio['Win/Loss ratio'] = club_win_loss_ratio['Wins'] / club_win_loss_ratio['Total Games'] * 100
        return (club_win_loss_ratio[club_win_loss_ratio['Total Games'] >= min_games].sort_values('Win/Loss ratio', ascending=False).index[0],round(club_win_loss_ratio[club_win_loss_ratio['Total Games'] >= min_games].sort_values('Win/Loss ratio', ascending=False).iloc[0][4],2))
    elif ground == 'Home':
        club_win_loss_ratio = pd.concat([club_wins_home, club_draws_home, club_loses_home], axis=1).fillna(0)
        club_win_loss_ratio.columns = ["Wins", "Draws", "Loses"]
        club_win_loss_ratio['Total Games'] = club_win_loss_ratio['Wins'] + club_win_loss_ratio['Draws']  + club_win_loss_ratio['Loses'] 
        club_win_loss_ratio.astype(int)
        club_win_loss_ratio['Win/Loss ratio'] = club_win_loss_ratio['Wins'] / club_win_loss_ratio['Total Games'] * 100
        return (club_win_loss_ratio[club_win_loss_ratio['Total Games'] >= min_games].sort_values('Win/Loss ratio', ascending=False).index[0],round(club_win_loss_ratio[club_win_loss_ratio['Total Games'] >= min_games].sort_values('Win/Loss ratio', ascending=False).iloc[0][4],2))
    elif ground == 'Away':
        club_win_loss_ratio = pd.concat([club_wins_away, club_draws_away, club_loses_away], axis=1).fillna(0)
        club_win_loss_ratio.columns = ["Wins", "Draws", "Loses"]
        club_win_loss_ratio['Total Games'] = club_win_loss_ratio['Wins'] + club_win_loss_ratio['Draws']  + club_win_loss_ratio['Loses'] 
        club_win_loss_ratio.astype(int)
        club_win_loss_ratio['Win/Loss ratio'] = club_win_loss_ratio['Wins'] / club_win_loss_ratio['Total Games'] * 100
        return (club_win_loss_ratio[club_win_loss_ratio['Total Games'] >= min_games].sort_values('Win/Loss ratio', ascending=False).index[0],round(club_win_loss_ratio[club_win_loss_ratio['Total Games'] >= min_games].sort_values('Win/Loss ratio', ascending=False).iloc[0][4],2))


@st.cache_data
def club_seasons(team):
    return int(results[(results['HomeTeam'] == team) | (results['AwayTeam'] == team)].count()['Game_ID']/38)

def own_goals_per_club(team, ground='All'):
    if ground == 'All':
        return club_own_goals_df[(((club_own_goals_df['HomeTeam'] == team) & (club_own_goals_df['Ground'] == 'Away')) & (club_own_goals_df['OwnGoal'] == 1)) |
                (((club_own_goals_df['AwayTeam'] == team) & (club_own_goals_df['Ground'] == 'Home')) & (club_own_goals_df['OwnGoal'] == 1))]['Game_ID'].count()
    elif ground == 'Home':
        return club_own_goals_df[(((club_own_goals_df['HomeTeam'] == team) & (club_own_goals_df['Ground'] == 'Away')) & (club_own_goals_df['OwnGoal'] == 1))]['Game_ID'].count()

    elif ground == 'Away':
        return club_own_goals_df[(((club_own_goals_df['AwayTeam'] == team) & (club_own_goals_df['Ground'] == 'Home')) & (club_own_goals_df['OwnGoal'] == 1))]['Game_ID'].count()

st.set_page_config(page_title='PL 2011/2012 - 2022/2023 Stats', page_icon='favicon.ico', layout='wide')

with open('styles.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

st.title('Premier League 2011/2012 - 2022/2023', anchor='page_title')

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    team = st.selectbox(label='Team',options=teams)
    st.subheader('')
with col2:
    ground = st.selectbox(options=['All', 'Home', 'Away'], label='Ground')
with col3:
    placeholder_1 = st.multiselect(label='A', options=['a'])
with col4:
    placeholder_2 = st.multiselect(label='B', options=['b'])
with col5:
    placeholder_3 = st.multiselect(label='C', options=['c'])

wins = most_wins_draws_loses(team=team, result_type='Wins', ground=ground)
draws = most_wins_draws_loses(team=team, result_type='Draws', ground=ground)
loses = most_wins_draws_loses(team=team, result_type='Loses', ground=ground)
seasons = int((most_wins_draws_loses(team=team, result_type='Wins') + most_wins_draws_loses(team=team, result_type='Draws') + most_wins_draws_loses(team=team, result_type='Loses')) / 38)
points = wins * 3 + draws
club_penalties = penalty_results(team=team, ground=ground)
club_goals_scored = team_goals(team=team, ground=ground)
club_goals_conceded = club_conceded_goals(team=team, ground=ground)
try:
    min_games = 5
    best_against = best_win_ratio(team=team, ground=ground)
except IndexError: #For teams that playted less than 3 seasons in Premier League, since they ahven't played anyone 5 times
    min_games = 0
    best_against = best_win_ratio(team=team, min_games=min_games, ground=ground)
own_goals = own_goals_per_club(team=team, ground=ground)

with col1:
    st.header('')
    st.header('')
    st.header('')
    st.image(f'Club logos/{team}.png', use_column_width='auto')

with col2:
    st.title(wins, anchor='team_wins')
    st.title(f'{wins/seasons:.2f}', anchor='stat_wins_more')
    st.title('average per season', anchor='small_stat_description')
    st.title('Wins', anchor='big_stat')
    st.title(club_goals_scored, anchor='stat')
    st.title(f'{round(club_goals_scored/seasons,2):.2f}', anchor='small_stat')
    st.title('average per season', anchor='small_stat_description')
    st.title('Scored', anchor='stat_description')
with col3:
    st.title(draws, anchor='team_draws')
    st.title(f'{draws/seasons:.2f}', anchor='stat_draws_more')
    st.title('average per season', anchor='small_stat_description')
    st.title('Draws', anchor='big_stat')
    st.title(club_goals_conceded, anchor='stat')
    st.title(f'{(club_goals_conceded/seasons):.2f}', anchor='small_stat')
    st.title('average per season', anchor='small_stat_description')
    st.title('Conceded', anchor='stat_description')
with col4:
    st.title(loses, anchor='team_loses')
    st.title(f'{loses/seasons:.2f}', anchor='stat_loses_more')
    st.title('average per season', anchor='small_stat_description')
    st.title('Loses', anchor='big_stat')
    st.title(club_penalties, anchor='stat')        
    st.title(f'{club_penalties/club_goals_scored*100:.2f}%', anchor='small_stat')
    st.title('of total goals scored', anchor='small_stat_description')
    st.title('Penalties', anchor='stat_description')
with col5:
    st.title(points, anchor='team_points')
    st.title(f'{points/seasons:.2f}', anchor='stat_points_more')
    st.title('average per season', anchor='small_stat_description')
    st.title('Points', anchor='big_stat')

    # Comment out this and comment the block below to show own golas instead of "best against"

    # st.title(f'{own_goals}', anchor='stat')        
    # st.title(f'{own_goals/club_goals_conceded*100:.2f}%', anchor='small_stat')
    # st.title('of total goals conceded', anchor='small_stat_description')
    # st.title('Own goals', anchor='stat_description')

    tab2_col4_col1, tab2_col4_col2, tab2_col4_col3 = st.columns([0.87,1,1])
    with tab2_col4_col2:
        st.image(f'Club logos/{best_against[0]}.png', use_column_width='auto')
    st.title(f'{best_against[1]:.2f}%', anchor='small_stat') 
    st.title('win percentage', anchor='small_stat_description')
    st.title('Best score', anchor='stat_description')
    st.title(f'(min {min_games} games)', anchor='small_stat_description')