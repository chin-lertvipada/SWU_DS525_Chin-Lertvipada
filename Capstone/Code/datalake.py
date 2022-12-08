from pyspark import SparkConf
from pyspark.sql import SparkSession


# cat ~/.aws/credentials
AWS_ACCESS_KEY_ID = "ASIAXZM22O2VR6ZR6QEV"
AWS_SECRET_ACCESS_KEY = "52M4QXCoteoU6K2p9WiJyVeNeWS7Q6VBhPEK4nfY"
AWS_SESSION_TOKEN = """
FwoGZXIvYXdzEC0aDNiNpfGSfyIpkFPsjSLMAYWX12ZnePa0bpOZNtcPttMyqcOB+qbYCDy/
fZM/n3WkvnuDWHyqDb4Z8sqfA0fevP7tVdWHA++Wt31R/pTHPPfJZn2E4LO9/3C418J80jPz
pae8F2TlrlW66sycCgewW0SphMVbxhxFcV2pTaBBLoA66oIMOZWewkj3IbPVDetdcowg0yfRK
3AjdxADl6aUxqAgfzktJvYtW58Ti3EkbkaiVjmO57YvsRubnuXVmDWcWRc0FAyHHiCdUev6Sq
sEd+0cYq8ndgbNuELSJijKm8ecBjIt3RcTQ3dn8Qlls3tjZP3CT19+gkHEoXTClJQSBkcm8WQhp7cozQBO6ng7XSsW
"""


conf = SparkConf()
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2")
conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
conf.set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
conf.set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
conf.set("spark.hadoop.fs.s3a.session.token", AWS_SESSION_TOKEN)


bucket = "s3a://jaochin-dataset-fifa"
landing_zone = f"{bucket}/landing/"
cleaned_zone = f"{bucket}/cleaned/"
# cleaned_zone = f"../Datalake/"


spark = SparkSession.builder.config(conf=conf).getOrCreate()


data = spark.read.option("header","true").option("multiline", "true").csv(landing_zone)
data.createOrReplaceTempView("staging_data")


table = spark.sql("""
    select
        sofifa_id
        , long_name
        , age
        , overall
        , value_eur
        , wage_eur
        , team_position
        , nationality
        , club_name
        , league_name
    from
        staging_data
""")
# table.show()


table_leagues = spark.sql("""
    select 
        row_number() over (order by league_name) as league_id
        , league_name 
        , current_date() as date_oprt
    from (
        select distinct league_name from staging_data where league_name is not null order by league_name
    )
""")
table_leagues.createOrReplaceTempView("leagues")
# table_leagues.show()


table_clubs = spark.sql("""
    select 
        row_number() over (order by club.club_name) as club_id
        , club.club_name 
        , league.league_id 
        , current_date() as date_oprt
    from (
        select distinct 
            club_name
            , league_name 
        from staging_data 
        where club_name is not null 
        order by club_name
    ) club
    inner join leagues league 
        on league.league_name = club.league_name
        and league.date_oprt = current_date()
""")
table_clubs.createOrReplaceTempView("clubs")
# table_clubs.show()


table_positions = spark.sql("""
    select 
        row_number() over (order by team_position) as position_id
        , team_position as position_name
        , current_date() as date_oprt
    from (
        select distinct team_position from staging_data where team_position is not null order by team_position
    )
""")
table_positions.createOrReplaceTempView("positions")
# table_positions.show()


table_nationalities = spark.sql("""
    select 
        row_number() over (order by nationality) as nationality_id
        , nationality as nationality_name 
        , current_date() as date_oprt
    from (
        select distinct nationality from staging_data where nationality is not null order by nationality
    )
""")
table_nationalities.createOrReplaceTempView("nationalities")
# table_nationalities.show()


table_players = spark.sql("""
    select 
        sofifa_id as player_id
        , replace(long_name, '"', '') as player_name
        , age as player_age
        , overall as player_overall
        , value_eur as player_value
        , wage_eur as player_wage
        , position.position_id
        , nationality.nationality_id
        , club.club_id
        , current_date() as date_oprt
    from (
        select distinct 
            sofifa_id
            , long_name
            , age
            , overall
            , value_eur
            , wage_eur
            , team_position
            , nationality
            , club_name
        from staging_data 
        order by sofifa_id
    ) player
    inner join positions position 
        on position.position_name = player.team_position
        and position.date_oprt = current_date()
    inner join nationalities nationality 
        on nationality.nationality_name = player.nationality
        and nationality.date_oprt = current_date()
    inner join clubs club 
        on club.club_name = player.club_name
        and club.date_oprt = current_date()
""")
table_players.createOrReplaceTempView("players")
# table_players.show()


table_leagues.write.partitionBy("date_oprt").mode("append").option("header",True).csv(cleaned_zone+"/leagues")
table_clubs.write.partitionBy("date_oprt").mode("append").option("header",True).csv(cleaned_zone+"/clubs")
table_positions.write.partitionBy("date_oprt").mode("append").option("header",True).csv(cleaned_zone+"/positions")
table_nationalities.write.partitionBy("date_oprt").mode("append").option("header",True).csv(cleaned_zone+"/nationalities")
table_players.write.partitionBy("date_oprt").mode("append").option("header",True).csv(cleaned_zone+"/players")
