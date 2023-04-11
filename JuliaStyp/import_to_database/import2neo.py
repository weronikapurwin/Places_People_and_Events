import re
from itertools import chain
import pandas as pd
from neo import Neo4jDB


def read_data(letters_csv, xml_csv):
    """Read and prepare information from csv files"""
    df_letters = pd.read_csv(letters_csv)
    df_letters.set_index('id', drop=False, inplace=True)
    df_letters.replace(to_replace=[float('nan'), '[nan]'], value=None, inplace=True)
    df_letters[['osoba_od', 'osoba_do']] = df_letters[['osoba_od', 'osoba_do']].apply(eval_row)

    df_xml = pd.read_csv(xml_csv)
    df_xml.replace(to_replace=float('nan'), value=None, inplace=True)
    cols = ['adresat', 'osoba_wzmiankowana', 'osoba_prawdopodobnie_wzmiankowana']
    df_xml[cols] = df_xml[cols].apply(lambda row: row.apply(norm_name), axis=1)
    df_xml['miejsce_wzmiankowane'] = df_xml['miejsce_wzmiankowane'].apply(norm_place)
    return df_letters, df_xml


def eval_row(row):
    return row.apply(lambda x: eval(x) if x else None)


def prepare_names(fun):
    def wrapper(values):
        if not values:
            return None
        values = eval(values)
        result = [fun(x) for x in values]
        return result

    return wrapper


@prepare_names
def norm_name(name):
    """Changes the order of name's parts"""
    name = name.replace('\'', '')
    pattern = re.compile(r'^([^,(]+)(,)? ?([^(]+)?( \(.+\))?$')
    if name != 'Ojciec macochy kompozytora, Anny z Tańkowskich Paderewskiej':
        parts = pattern.findall(name)
        if parts:
            parts = parts[0]
            if parts[1]:
                if not ('Queen' in parts[2] or 'Duchess' in parts[2] or 'Prince' in parts[2]):
                    name = f'{parts[2]} {parts[0]}{parts[3]}'
    return name.strip()


@prepare_names
def norm_place(name):
    """Changes the order of name's parts"""
    name = name.replace('\'', '')
    pattern = re.compile(r'^([0-9]+)?([^(,]+)?(, )?([^(]+)?( \([^0-9]+\))?( \(.+\))?$')
    parts = pattern.findall(name)
    if parts:
        parts = parts[0]
        if parts[0]:
            name = f'{parts[0]} {parts[3]}'
        elif parts[3]:
            name = f'{parts[3]} {parts[1]}'
        elif parts[-1]:
            name = " ".join(parts[0:-1])
    return name.strip()


# def polish_title(row):
#     info = re.findall(r'(([Ll]istu? )?(.+))?([dD]o (.+) w (.+)$|[dD]o (.+))$', row[0])
#     if not info:
#         return pd.Series([None, None, None])
#     info = info[0]
#     sender = info[2]
#     addressee = info[-1] if info[-1] else info[4]
#     place = info[5]
#     result = pd.Series([sender, addressee, place])
#     return result.replace('', None)


def name_info():
    """Replace None values with information from the letter name"""
    idx = letters.query('osoba_od.isna() or osoba_do.isna()').index
    names = pd.read_csv(r'csv_data\letter_names_en.csv', index_col='id')
    names = names[names.index.isin(idx)]
    missing = pd.DataFrame()
    missing[['osoba_od', 'osoba_do', 'miejsce_do']] = names[['name']].apply(english_title, axis=1)
    return missing.replace({'osoba_od': {'Fr. Chopin': 'Fryderyk Chopin'},
                            'osoba_do': {'Fr. Chopin': 'Fryderyk Chopin'},
                            'miejsce_do': {'Paris': 'Paryż', 'Leipzig': 'Lipsk', 'London': 'Londyn'}})


def english_title(row):
    """Split the english name into sender, addressee and place"""
    info = re.findall(r'(.*[Ff]rom (.+) )?((.+)?([Tt]o (.+) in (.+)$|[Tt]o (.+))$)', row[0])
    if not info:
        return pd.Series([None, None, None])
    info = info[0]
    sender = info[1] if info[1] else info[3]
    sender = [sender.strip()] if sender else None
    addressee = info[-1] if info[-1] else info[5]
    if 'unknown' in addressee or '?' in addressee:
        addressee = None
    else:
        addressee = [addressee]
    place = info[6] if info[6] else None
    return pd.Series([sender, addressee, place])


def unique_persons():
    """Returns unique persons from both letters and xml_information"""
    people = set()
    for i in (letters['osoba_do'], letters['osoba_od'], xml_info['adresat'], xml_info['osoba_wzmiankowana']):
        people.update(chain.from_iterable(i.dropna()))
    return list(people)


def unique_places():
    """Returns unique places from both letters and xml_information"""
    places = set(letters['miejsce_od'].dropna())
    places.update(letters['miejsce_do'].dropna())
    places.update(chain.from_iterable(xml_info['miejsce_wzmiankowane'].dropna()))
    return list(places)


if __name__ == "__main__":
    letters, xml_info = read_data(r'csv_data\letters_prepared.csv', r'csv_data\letters_xml.csv')
    """Update the data with information from the english name"""
    # from_title = name_info()
    # for i in ('osoba_od', 'osoba_do', 'miejsce_do'):
    #     letters[i] = letters[i].combine_first(from_title[i])
    # letters.to_csv(r'csv_data\letters_prepared.csv', index=False)

    """Import CSV data into the database"""
    # persons = unique_persons()
    # places = unique_places()
    # print(f'* {len(persons)} *', *sorted(persons), sep='\n', end='\n\n')
    # print(f'* {len(places)} *', *sorted(places), sep='\n')

    neo = Neo4jDB()
    # neo.clear()
    neo.add_persons(data=unique_persons())
    neo.add_places(data=unique_places())
    neo.add_letters(data=letters)
    neo.add_xml_info(data=xml_info)
    neo.close()
