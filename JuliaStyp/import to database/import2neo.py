import pandas as pd
import re
from itertools import chain
from neo import Neo4jDB


def read_data(letters_csv, xml_csv):
    """Read and prepare information from csv files"""
    df_letters = pd.read_csv(letters_csv)
    df_letters.replace(to_replace=[float('nan'), '[nan]'], value=None, inplace=True)
    df_letters['osoba_od'] = df_letters['osoba_od'].apply(lambda x: eval(x) if x else None)

    df_xml = pd.read_csv(xml_csv)
    df_xml.replace(to_replace=float('nan'), value=None, inplace=True)
    cols = ['adresat', 'osoba_wzmiankowana', 'osoba_prawdopodobnie_wzmiankowana']
    df_xml[cols] = df_xml[cols].apply(lambda row: row.apply(norm_name), axis=1)
    df_xml['miejsce_wzmiankowane'] = df_xml['miejsce_wzmiankowane'].apply(norm_place)
    return df_letters, df_xml


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


def unique_persons():
    """Returns unique persons from both letters and xml_information"""
    people = set(letters['osoba_do'].dropna())
    people.update(chain.from_iterable(letters['osoba_od'].dropna()))
    people.update(chain.from_iterable(xml_info['adresat'].dropna()))
    people.update(chain.from_iterable(xml_info['osoba_wzmiankowana'].dropna()))
    return list(people)


def unique_places():
    """Returns unique places from both letters and xml_information"""
    places = set(letters['miejsce_od'].dropna())
    places.update(letters['miejsce_do'].dropna())
    places.update(chain.from_iterable(xml_info['miejsce_wzmiankowane'].dropna()))
    return list(places)


def title_info(row):
    info = re.findall(r'[dD]o (.+) w (.+)$|[dD]o (.+)$', row[0])
    if not info:
        return pd.Series([None, None])
    info = info[0]
    person = info[0] if info[0] else info[-1]
    place = info[1]
    return pd.Series([person, place if place else None])


if __name__ == "__main__":
    letters, xml_info = read_data('letters_prepared.csv', 'letters_xml.csv')
    print(*sorted(unique_persons()), sep='\n', end='\n\n')
    print(*sorted(unique_places()), sep='\n')

    # letters[['person_do', 'place_do']] = letters[['name']].apply(title_info, axis=1)
    # letters[['id', 'name', 'osoba_od', 'osoba_do', 'person_do', 'miejsce_do','place_do']].to_csv('title.csv')

    neo = Neo4jDB()
    # neo.clear()
    neo.add_persons(data=unique_persons())
    neo.add_places(data=unique_places())
    neo.add_letters(data=letters)
    neo.add_xml_info(data=xml_info)
    neo.close()

"""
PROBLEMY
- różne zapisy:
    Feliks Mendelssohn-Bartholdy
    Felix Mendelssohn-Bartholdy
        Ernest Legouvé
        Ernest Legouvė
    Marie d Agoult (hrabina)
    Marie d’Agoult
    
- uzupełnienia?:
    Adèle Hugo
    Adèle Hugo (Madame Hugo)
        Anna Czartoryska
        Anna Czartoryska (księżna)
    
- miejsca:
    Dresden
    Drezno
        Edinburgh
        Edynburg
    Leipzig
    Lipsk
        Valdemosa koło Palmy
        Valldemosa
"""
