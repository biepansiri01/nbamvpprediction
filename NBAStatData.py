import pandas as pd
import os

class NBAStatData:

  def __init__(self, year_start, year_stop,return_csv):

    self.year_start = year_start
    self.year_stop = year_stop
    self.return_csv = return_csv
    self.mvp_path = "/content/drive/MyDrive/NBAdataframe/mvp"
    self.basic_stat_path = "/content/drive/MyDrive/NBAdataframe/basicstat"
    self.advanced_stat_path = "/content/drive/MyDrive/NBAdataframe/advancedstat"
    self.teamstat_path = "/content/drive/MyDrive/NBAdataframe/teamstat"
    self.playerstat = "/content/drive/MyDrive/NBAdataframe/playerstat"
    self.mvp_voting_path = "/content/drive/MyDrive/NBAdataframe/voting"




  def cleanColMVP(self, dataframe):
    new_col = [] 
    for col in dataframe.columns:
      new_col.append(col[1])
    dataframe.columns = new_col
    dataframe = dataframe.drop(columns = ["Rank"], axis=1)
    return dataframe


  def mvp_dataframe(self):
    mvpcandidate_df = {}

    for year in range(self.year_start, self.year_stop+1): 
      url = "https://www.basketball-reference.com/awards/awards_{0}.html#mvp".format(year)
      mvpcandidate_df[year] = pd.read_html(url)[0]

      """
      ตัด column name ที่ซ้อนกันออก ให้เหลือแต่ column name ที่เป็น feature
      """
      mvpcandidate_df[year] = self.cleanColMVP(mvpcandidate_df[year])
      mvpcandidate_df[year]["Player"] = mvpcandidate_df[year]["Player"].str.replace("*","")
      


      #to csv
      if self.return_csv == True:
        mvpcandidate_df[year].to_csv(os.path.join(self.mvp_path, "mvp_candidate_SS({0}-{1}).csv".format(year-1,year)), index=True)  

    return mvpcandidate_df


  def basic_stat(self):
    # stat โดยรวมของผู้เล่นในลีคแต่ละคน

    total_player_df = {}

    for year in range(self.year_start, self.year_stop+1): 
      url = "https://www.basketball-reference.com/leagues/NBA_{0}_totals.html".format(year)
      total_player_df[year] = pd.read_html(url)[0]

      total_player_df[year]["Player"] = total_player_df[year]["Player"].str.replace("*","")

      droprow_cond = total_player_df[year][(total_player_df[year]["Rk"] =="Rk")].index
      total_player_df[year].drop(droprow_cond, inplace=True)


      total_player_df[year] = total_player_df[year].drop(columns = ["Rk","GS"]) #GS [Game Started : การเป็นตัวจริง] ไม่มีผลต่อการหาผู้เล่นที่ดีที่สุด

      numberic_col = total_player_df[year].iloc[:,-24:].columns
      for col in numberic_col:
        total_player_df[year][col] = pd.to_numeric(total_player_df[year][col],errors='coerce')
      
      
      #to csv
      if self.return_csv == True:
        total_player_df[year].to_csv(os.path.join(self.basic_stat_path,'TOTAL_Player_SS({0}-{1}).csv'.format(year-1,year)), index=True)  

    return total_player_df


  def advanced_stat(self):
      # Advanced stat ของผู้เล่นแต่ละคน

    advanced_player_df = {}

    for year in range(self.year_start, self.year_stop+1): 
      url = "https://www.basketball-reference.com/leagues/NBA_{0}_advanced.html".format(year)
      advanced_player_df[year] = pd.read_html(url)[0]

      #mvpcandidate_df[year] = cleanCol(mvpcandidate_df[year])
      advanced_player_df[year]["Player"] = advanced_player_df[year]["Player"].str.replace("*","")

      droprow_cond = advanced_player_df[year][(advanced_player_df[year]["Rk"] =="Rk")].index

      advanced_player_df[year].drop(droprow_cond, inplace=True)
      
      advanced_player_df[year] = advanced_player_df[year].drop(columns = ["Unnamed: 19", "Unnamed: 24"]) # remove empty columns
      advanced_player_df[year] = advanced_player_df[year].drop(columns = ["Rk",'Pos', 'Age', 'G', 'MP']) # remove columns already included in regular stats csv

      numberic_col = advanced_player_df[year].iloc[:,-20:].columns
      for col in numberic_col:
        advanced_player_df[year][col] = pd.to_numeric(advanced_player_df[year][col],errors='coerce')


      #to csv
      if self.return_csv == True:
        advanced_player_df[year].to_csv(os.path.join(self.advanced_stat_path,'ADVANCED_Player_SS({0}-{1}).csv'.format(year-1,year)), index=True) 

    return advanced_player_df 


  def compress_multirowplayers(self, df):
    """
    This function removes extra rows for players with more than one row (because they switched teams during the season): 
    For players with more than one row, keep only row with 'Tm' == 'TOT' and replace 'Tm' value with most recent team id:
    """
    """
    TOT : it stands for total. it means he played for two (or more) different teams that year. 'TOT' is just the cumulative 
    score from all of the teams he played for that year.
    """

    indices_of_rows_toberemoved = []
    player_team_dict = {}

    previous_player_id = ''
    
    for index, row in df.iterrows():
        player_id = row['Player']
        if (player_id == previous_player_id):
            if (row['Tm'] != 'TOT'):
                indices_of_rows_toberemoved.append(index)
                player_team_dict[player_id] = row['Tm'] # this works because the last team in the list is the player's most recent team 
        previous_player_id = player_id

    for index in indices_of_rows_toberemoved:
        df.drop(index, inplace=True)

    for index, row in df.iterrows():
        for key, value in player_team_dict.items():
            if (row['Player'] == key):
                df.at[index, 'Tm'] = value

    return df

  
  def player_stat(self):
    player_stat_df = {}
    
    basic_stat = self.basic_stat( )
    advanced_stat = self.advanced_stat( )

    
    for year in range(self.year_start, self.year_stop+1): 
    
      df = pd.merge(basic_stat[year], advanced_stat[year], how='left', left_on=['Player', 'Tm'], right_on=['Player', 'Tm'])
      
      df = self.compress_multirowplayers(df)

      player_stat_df[year] = df
    
    return player_stat_df


  @property
  def team_initials(self):
    #ชื่อย่อทีม
    teamname ={
        'ATL':'Atlanta Hawks',
        'BOS':'Boston Celtics',
        'BRK':'Brooklyn Nets',
        'CHA':'Charlotte Bobcats',
        'CHO':'Charlotte Hornets',
        'CHH':'Charlotte Hornets',
        'CHI':'Chicago Bulls',
        'CLE':'Cleveland Cavaliers',
        'DAL':'Dallas Mavericks',
        'DEN':'Denver Nuggets',
        'DET':'Detroit Pistons',
        'GSW':'Golden State Warriors',
        'HOU':'Houston Rockets',
        'IND':'Indiana Pacers',
        'KCK':'Kansas City Kings',
        'LAC':'Los Angeles Clippers',
        'LAL':'Los Angeles Lakers',
        'MEM':'Memphis Grizzlies',
        'MIA':'Miami Heat',
        'MIL':'Milwaukee Bucks',
        'MIN':'Minnesota Timberwolves',
        'NJN':'New Jersey Nets',
        'NOH':'New Orleans Hornets',
        'NOK':'New Orleans/Oklahoma City Hornets',
        'NOP':'New Orleans Pelicans',
        'NYK':'New York Knicks',
        'OKC':'Oklahoma City Thunder',
        'ORL':'Orlando Magic',
        'PHI':'Philadelphia 76ers',
        'PHI':'Philadelphia 76ers\xa0(1)',
        'PHO':'Phoenix Suns',
        'POR':'Portland Trail Blazers',
        'SAC':'Sacramento Kings',
        'SAS':'San Antonio Spurs',
        'SDC':'San Diego Clippers',
        'SEA':'Seattle SuperSonics',
        'TOR':'Toronto Raptors',
        'UTA':'Utah Jazz',
        'VAN':'Vancouver Grizzlies',
        'WAS':'Washington Wizards',
        'WSB':'Washington Bullets',
      }
    return teamname

  
  def team_stat(self):
    teamstat_df = {}

    for year in range(self.year_start, self.year_stop+1): 

      url = "https://www.basketball-reference.com/leagues/NBA_{0}.html#misc_stats".format(year)
     
      ec_team = pd.read_html(url)[0].iloc[:,:4]
      wc_team = pd.read_html(url)[1].iloc[:,:4]



      ec_team.rename(columns={'Eastern Conference': 'Tm'}, inplace=True)
      wc_team.rename(columns={'Western Conference': 'Tm'}, inplace=True)
 

      teamstat_df[year] = pd.concat([ec_team, wc_team]).reset_index(drop=True)

      teamstat_df[year]["Tm"] = teamstat_df[year]["Tm"].str.replace("*","")




      teamstat_df[year] = teamstat_df[year][~teamstat_df[year]["Tm"].str.contains("Division")]
   
      numberic_col = teamstat_df[year].iloc[:,1:].columns
    
      for col in numberic_col:
        teamstat_df[year][col] = pd.to_numeric(teamstat_df[year][col],errors='coerce')

    
      team=[]
      for tm in teamstat_df[year]["Tm"]:
        team.append(list(self.team_initials.keys())[list(self.team_initials.values()).index(tm)])

     
      teamstat_df[year]["Tm"] = team
      
      if self.return_csv == True:
        teamstat_df[year].to_csv(os.path.join(self.teamstat_path,'TEAM_STAT({0}-{1}).csv'.format(year-1,year)), index=True) 
    
    return teamstat_df

  
  def teamwin_dict(self, df):
    TeamWins_dict = {}
    for index, row in df.iterrows():
      TeamWins_dict[row['Tm']] = row['W']
    return TeamWins_dict


  def teamtotal_dict(self, df):
    TeamTotal_dict = {}
    for index, row in df.iterrows():
      TeamTotal_dict[row['Tm']] = row['W']+row['L']
    return TeamTotal_dict


  def player_with_teamstat(self):
    player_stat_withteam = {}
    playerstat = self.player_stat( )
    teamstat = self.team_stat( )
    

    for year in range(self.year_start, self.year_stop+1): 
      teamwin_column = []
      teamtotal_column = []

      player = playerstat[year]
      team = teamstat[year]

  
      teamwin_dict = self.teamwin_dict(team)
      teamtotal_dict = self.teamtotal_dict(team)

      for team in player["Tm"]:
        #teamwin_column.append(teamwin_dict(teamstat)[team])
        try:
          teamwin_column.append(teamwin_dict[team])
          teamtotal_column.append(teamtotal_dict[team])
        except KeyError:
          teamwin_column.append(teamwin_dict["CHO"])
          teamtotal_column.append(teamtotal_dict["CHO"])


      player["TW"] = teamwin_column
      player["TT"] = teamtotal_column

      player_stat_withteam[year] = player.fillna(0)

    return player_stat_withteam


  def addspecialPlayerStat(self):
    player_df = {}

    player_stat_withteam = self.player_with_teamstat( )


    feature = ["Player","WIN%","G/TOT","MP/G","PTS","AST","TRB","STL","BLK","PF","FGA","3PA","FTA","FG%","3P%","FT%","ORB%","DRB%","TRB%","AST%","STL%","BLK%","TOV%"	
    ,"PER","BPM","TS%","USG%","WS","WS/48"]

    

    for year in range(self.year_start, self.year_stop+1): 
      player_df[year] = pd.DataFrame(columns=feature)  

      for item in feature:
        if item == "WIN%":
          player_df[year]["WIN%"] = player_stat_withteam[year]["TW"]/player_stat_withteam[year]["TT"]

        elif item == "G/TOT":
          player_df[year]["G/TOT"] = player_stat_withteam[year]["G"]/player_stat_withteam[year]["TT"]

        elif item == "MP/G":
          player_df[year]["MP/G"] = player_stat_withteam[year]["MP"]/player_stat_withteam[year]["G"]
        
        else:
          player_df[year][item] = player_stat_withteam[year][item]

      player_df[year]["YEAR"] = year
            
      if self.return_csv == True:
        player_df[year].to_csv(os.path.join(self.playerstat,'PLAYER_STAT({0}-{1}).csv'.format(year-1,year)), index=True) 

    return player_df

  
  def mvp_votingshare(self):
    mvp_share = {}
    mvpcandidate_df = self.mvp_dataframe()
    player_df = self.addspecialPlayerStat()

    for year in range(self.year_start, self.year_stop+1):
      mvpcandidate_df[year] = mvpcandidate_df[year][["Player","Share"]] 
      mvpcandidate_df[year].rename(columns={"Share": "Voting_Share"},inplace=True)
      mvp_share[year] = pd.merge(player_df[year], mvpcandidate_df[year], how='left', left_on=['Player'], right_on=['Player'])
      mvp_share[year] = mvp_share[year].fillna(0) 

      if self.return_csv == True:
        mvp_share[year].to_csv(os.path.join(self.mvp_voting_path,'MVP_VOTING_SHARE({0}-{1}).csv'.format(year-1,year)), index=True) 

    return mvp_share
