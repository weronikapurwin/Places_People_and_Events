import pandas as pd
import re
import requests
from bs4 import BeautifulSoup

LINK = 'https://api.nifc.pl/v2/index.php/letters'


def get_letters(link, params):
    response = requests.post(link, json=params)
    txt = response.text.replace('null', 'None')
    return eval(txt)


def search_letters(offset=None, limit=None, **kwargs):
    """Returns letters as pd.DataFrame
    **kwargs:
        return_actors
        return_composers
        return_audio
        return_senders
        return_addressees
    """
    off = f'/{offset}' if offset else r'/0'
    lim = f'/{limit}' if limit else ''
    letters = get_letters(f'{LINK}/search_letters{off}{lim}', kwargs)
    df = pd.DataFrame.from_records(letters).set_index('id')
    df.index = df.index.astype('int')
    return df


# more information and xml
def letter_by_id(letter_id, **kwargs):
    """Returns one letter as pd.Series
    **kwargs:
        return_actors
        return_composers
        return_audio
        return_senders
        return_addressees
        return_galleries: return letter scan
        return_materials: return letter materials
        return_techniques: return letter techniques
    """
    letter = get_letters(f'{LINK}/get_letter_by_id/{letter_id}', kwargs)
    if letter and letter.get('xml'):
        letter['xml'] = letter['xml'].replace(r'\/', '/')
    return pd.Series(letter)


def get_xml(letter):
    """
    adresat listu
    osoba wzmiankowana
    miejsce wzmiankowane
    """
    xml = BeautifulSoup(letter['xml'], features='xml')
    tags = [i for i in xml.find_all('marc:subfield') if
            i['code'] == 'e' and i.contents[0] != 'Narodowy Instytut Fryderyka Chopina']
    result = {}
    for i in tags:
        name = i.parent.find('marc:subfield')
        result.setdefault(i.contents[0], []).append(*name.contents)
    return pd.Series(result, dtype='object')


def xml_letters(letters):
    """Returns information from xml as pd.DataFrame"""
    result = letters[['xml']].dropna().apply(get_xml, axis=1)
    result['adresat listu'] = result['adresat listu'].combine_first(result['adresat'])
    result.drop(columns=['adresat', 'społeczność wzmiankowana'], inplace=True)
    columns = {'adresat listu': 'adresat',
               'osoba prawdopodobnie wzmiankowana': 'osoba_prawdopodobnie_wzmiankowana',
               'osoba wzmiankowana': 'osoba_wzmiankowana',
               'miejsce wzmiankowane': 'miejsce_wzmiankowane'}
    result.rename(columns, axis=1, inplace=True)
    return result.drop(1500)  # drop empty value


def prepare_name(row):
    if not row:
        return None
    return [" ".join(i.split(",")[::-1]).strip() for i in row.split(';')]


def get_authors(letters):
    """Combines information about sender from columns: author, persons and sender_name"""
    names = letters['author'].apply(prepare_name)
    persons = letters['persons'].apply(lambda x: [i['name'] for i in eval(x)] if x else None)
    names = names.combine_first(persons)
    senders = letters['sender_name'].apply(lambda row: [row] if row else None)
    names.rename('osoba_od', inplace=True)
    return names.combine_first(senders)


def read_places():
    """Returns place names and ids as dictionary"""
    with open('places.csv', 'r', encoding='UTF-8') as file:
        lines = file.readlines()[1::]
    return {int(i): j.strip() for i, j in [x.split(',') for x in lines]}


def text(txt):
    """Removes tags from text"""
    if not txt:
        return None
    result = re.sub(r'<.*?>', ' ', txt)
    return re.sub(r' +', ' ', result)


def get_text(letters):
    """Combines text from columns: txt and original_txt
       Removes tags from txt and summary"""
    letters['txt'] = letters['txt'].combine_first(letters['original_txt'])
    return letters[['txt', 'summary']].apply(lambda row: row.apply(text))


def prepared_letters(letters):
    """Returns information from letters as pd.DataFrame"""
    addressee = letters['addressee_name'].combine_first(letters['osoba_do_txt'])
    addressee.rename('osoba_do', inplace=True)
    selected = letters.loc[:, ['data_start', 'data_stop', 'data_txt', 'name']]
    place_names = read_places()
    places = letters[['miejsce_od_id', 'miejsce_do_id']].apply(lambda row: row.apply(place_names.get))
    places.rename({'miejsce_od_id': 'miejsce_od', 'miejsce_do_id': 'miejsce_do'}, axis=1, inplace=True)
    return pd.concat([get_authors(letters), addressee, places, selected, get_text(letters)], axis=1)


if __name__ == '__main__':
    """ZAPIS WSZYTSKICH LISTÓW DO CSV"""
    # all_let = search_letters(offset=0, limit=1050)
    # lett_list = [letter_by_id(i, return_senders=True, return_addressees=True) for i in all_let.index]
    # pd.DataFrame(lett_list).to_csv('all_letters.csv')

    all_letters = pd.read_csv('all_letters.csv', index_col='id')
    all_letters.replace(to_replace=[float('nan'), '[nan]'], value=None, inplace=True)

    """WYSZUKANIE ISTOTNYCH INFORMACJI I ZAPIS DO CSV"""
    prepared = prepared_letters(all_letters)
    # prepared.to_csv('letters_prepared.csv')
    xletters = xml_letters(all_letters)
    # xletters.to_csv('letters_xml.csv')
