from dataclasses import dataclass
from typing import Generator

from utils import File, Hash, Log, Parallel

from scraper import AbstractDoc
from utils_future import WWW

log = Log('PMDPressRelease')


@dataclass
class PMDPressRelease(AbstractDoc):
    URL_BASE = 'https://pmd.gov.lk'

    LANG_TO_URL_BASE_LANG = {
        'en': f'{URL_BASE}/news-media',
        'si': f'{URL_BASE}/si/පුවත්',
        'ta': f'{URL_BASE}/ta/செய்தி-ஊடக-அறிக்கை',
    }

    LANG_TO_NEXT_TEXT = {
        'en': 'Next Page »',
        'si': 'ඉදිරි පිටුවට »',
        'ta': 'அடுத்த பக்கம் »',
    }

    @classmethod
    def get_doc_class_label(cls):
        return 'lk_pmd_press_releases'

    @classmethod
    def get_doc_class_description(cls):
        return "A Sri Lanka Presidential Media Division press release shares official updates on national decisions, policies, or events. It’s vital as the authoritative source ensuring transparency and public awareness."  # noqa: E501

    @classmethod
    def get_doc_class_emoji(cls):
        return '📢'

    @classmethod
    def scrape_pmd_article(cls, url: str) -> tuple[str, str]:
        www = WWW(url)
        soup = www.soup
        assert soup
        div = soup.find('div', class_='post-inner')
        h2 = div.find('h2')
        article_title = h2.text.strip()
        article_body_paragraphs = []
        for p in div.find_all('p'):
            article_body_paragraphs.append(p.text.strip())
        return article_title, article_body_paragraphs

    @classmethod
    def parse_article(cls, div, lang, num_set) -> 'PMDPressRelease':
        h4 = div.find('h4')
        description = h4.text.strip()
        a = h4.find('a')
        url = a['href']
        span_date = div.find('span', class_='timeline-date')
        m_part, d_part, y_part = [int(x) for x in span_date.text.split('-')]
        assert 1 <= m_part <= 12, m_part
        assert 1 <= d_part <= 31, d_part
        assert 0 <= y_part <= 99, y_part
        date_str = f'20{y_part:02d}-{m_part:02d}-{d_part:02d}'
        assert (
            len(date_str) == 10 and date_str[4] == '-' and date_str[7] == '-'
        ), date_str
        num = f'{lang}-{Hash.md5(description)[:6]}'

        if num in num_set:
            return None

        return cls(
            num=num,
            date_str=date_str,
            description=description,
            url_metadata=url,
            lang=lang,
        )

    @classmethod
    def get_docs_for_page(
        cls, lang, i_page: int, num_set: set[str]
    ) -> list['PMDPressRelease']:
        log.info(f'{lang=}, {i_page=:,}')
        url_base_lang = cls.LANG_TO_URL_BASE_LANG[lang]
        url = f'{url_base_lang}/page/{i_page}/'
        www = WWW(url)
        soup = www.soup
        assert soup
        divs = soup.find_all('div', class_='post_row')

        next_text = cls.LANG_TO_NEXT_TEXT[lang]
        a_next_page = soup.find('a', text=next_text)
        has_no_next_page = a_next_page is None

        doc_list = Parallel.map(
            lambda div: cls.parse_article(div, lang, num_set),
            divs,
            max_threads=cls.MAX_THREADS,
        )
        doc_list = [doc for doc in doc_list if doc is not None]

        return has_no_next_page, doc_list

    @classmethod
    def gen_docs(cls) -> Generator['PMDPressRelease', None, None]:
        doc_list = cls.list_all()
        num_set = (
            set([doc.num for doc in doc_list]) if doc_list else set()
        )  # HACKY

        completed_lang_set = set()
        i_page = 1
        while True:
            no_new_docs = True
            for lang in ['si', 'en', 'ta']:
                if lang in completed_lang_set:
                    continue
                has_no_next_page, docs_list = cls.get_docs_for_page(
                    lang, i_page, num_set
                )
                if has_no_next_page:
                    completed_lang_set.add(lang)
                for doc in docs_list:
                    no_new_docs = False
                    yield doc

            if no_new_docs:
                return

            i_page += 1

    def extract_text(self):
        if self.has_text:
            return
        article_title, article_body_paragraphs = self.scrape_pmd_article(
            self.url_metadata
        )
        content = '\n\n'.join(
            [
                self.description,
                article_title,
            ]
            + article_body_paragraphs
        )
        File(self.text_path).write(content)
        log.info(f"Wrote {self.text_path}")
