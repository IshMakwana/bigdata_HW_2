# Data Generation

from random import randint
import pandas as pd
from faker import Faker
from faker.providers import DynamicProvider

MOVIE_GENRE_DATA_URL = 'https://raw.githubusercontent.com/calvdee/viz-movies/master/data/recsys-data-movie-genres.csv'

def getMovieGenres(url):
    df = pd.read_csv(url, header=None)

    ustripped_list = list(df[1].replace('(\t)+', ' ', regex=True).unique())
    ustripped_list.sort()
    return [s.strip() for s in ustripped_list]

MOVIE_GENRE_PROVIDER = DynamicProvider(
     provider_name="movie_genre",
     elements=getMovieGenres(MOVIE_GENRE_DATA_URL),
)

fake = Faker()
fake.add_provider(MOVIE_GENRE_PROVIDER)


def generate_data(rows, users, session):
    data = pd.DataFrame(index=range(rows), columns=['UserID', 'UserName', 'WatchedMovie',
                                 'MovieGenre', 'SessionLength', 'LastLoginDate'])
    user_map = {}
    for i in range(users):
        user_map[i] = fake.unique.name()

    for i in range(rows):
        id = randint(1, users)
        watchedMovie = fake.boolean()
        data.loc[i, 'UserID'] = id
        data.loc[i, 'UserName'] = user_map[id-1]
        data.loc[i, 'WatchedMovie'] = watchedMovie
        data.loc[i, 'MovieGenre'] = (fake.movie_genre() if watchedMovie else '')
        data.loc[i, 'SessionLength'] = randint(1, session)
        data.loc[i, 'LastLoginDate'] = fake.date_this_century()
    return data

NUM_ROWS = 200000
NUM_USERS = 50000
MAX_SESSION = 1440 # Minutes in one day
df = generate_data(NUM_ROWS, NUM_USERS, MAX_SESSION)
# df

df.to_csv("hw_2_user_data.csv")
