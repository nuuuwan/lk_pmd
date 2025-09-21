from typing import Generator

from utils import Hash, Log, TimeFormat

from scraper import AbstractDoc
from utils_future import WWW

log = Log('PMDPressRelease')


class PMDPressRelease(AbstractDoc):
    URL_BASE = 'https://pmd.gov.lk'

    LANG_TO_URL_BASE_LANG = {
        'en': f'{URL_BASE}/news-media',
        'si': f'{URL_BASE}/si/පුවත්',
        'ta': f'{URL_BASE}/ta/செய்தி-ஊடக-அறிக்கை',
    }

    @classmethod
    def get_doc_class_label(cls):
        return 'lk_pmd_press_release'

    @classmethod
    def get_doc_class_description(cls):
        return "A Sri Lanka Presidential Media Division press release shares official updates on national decisions, policies, or events. It’s vital as the authoritative source ensuring transparency and public awareness."  # noqa: E501

    @classmethod
    def get_doc_class_emoji(cls):
        return '📢'

    @classmethod
    def gen_docs_for_page(
        cls, lang, i_page: int
    ) -> Generator['PMDPressRelease', None, None]:
        url_base_lang = cls.LANG_TO_URL_BASE_LANG[lang]
        url = f'{url_base_lang}/page/{i_page}/'
        www = WWW(url)
        soup = www.soup
        if not soup:
            return
        divs = soup.find_all('div', class_='post_row')
        for div in divs:
            h4 = div.find('h4')
            description = h4.text.strip()
            a = h4.find('a')
            url = a['href']
            span_date = div.find('span', class_='timeline-date')
            date_str_mm_dd_yyyy = span_date.text.strip()
            if len(date_str_mm_dd_yyyy) == 9:
                date_str_mm_dd_yyyy = (
                    date_str_mm_dd_yyyy[:3] + '0' + date_str_mm_dd_yyyy[3:]
                )
            assert (
                len(date_str_mm_dd_yyyy) == 10
                and date_str_mm_dd_yyyy[2] == '-'
                and date_str_mm_dd_yyyy[5] == '-'
            ), date_str_mm_dd_yyyy
            date_str = TimeFormat.DATE.format(
                TimeFormat('%m-%d-%Y').parse(date_str_mm_dd_yyyy),
            )
            hash_description = Hash.md5(description)[:6]
            yield cls(
                num=f'{date_str}-{hash_description}',
                date_str=date_str,
                description=description,
                url_metadata=url,
                lang=lang,
            )

    @classmethod
    def gen_docs_for_lang(
        cls, lang: str
    ) -> Generator['PMDPressRelease', None, None]:
        i_page = 1
        while True:
            has_docs = False
            for doc in cls.gen_docs_for_page(lang, i_page):
                yield doc
                has_docs = True

            if not has_docs:
                return

            i_page += 1

    @classmethod
    def gen_docs(cls) -> Generator['PMDPressRelease', None, None]:
        for lang in ['en', 'si', 'ta']:
            yield from cls.gen_docs_for_lang(lang)
