import argparse
import errno
import logging
import os
import re
import sys
import textwrap
import tqdm

import bs4
import requests

from whoosh import fields as whoosh_fields
from whoosh import index as whoosh_index
from whoosh import qparser as whoosh_parser

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

log = logging.getLogger()


INDEX_URL = "https://nips.cc/Conferences/2019/Schedule"
EVENT_URL_PATTERN = "https://nips.cc/Conferences/2019/Schedule?showEvent={event_id}"
SPEAKER_URL_PATTERN = (
    "https://nips.cc/Conferences/2019/Schedule?showSpeaker={speaker_id}"
)
CACHED_INDEX = "cache/index"
DATA_DIR = "data"
EVENT_ID_P = re.compile(r"onClick=\"showDetail\(([0-9]+)\)\"")
SPEAKER_ID_P = re.compile(r"onClick=\"showSpeaker\('([0-9-]+)'\);\"")


def main():
    args = _parse_args()
    if args.print_docs:
        _print_docs()
    elif args.find:
        _find(args.find)
    else:
        _build_index(args)


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-p", "--print-docs", action="store_true", help="print index contents"
    )
    p.add_argument("-f", "--find", metavar="TEXT", help="search for TEXT")
    p.add_argument("-m", "--max-events", type=int, help="max number of events to index")
    return p.parse_args()


def _print_docs():
    data = _init_data()
    with data.reader() as reader:
        results = [fields for _id, fields in reader.iter_docs()]
        _print_results(results)


def _print_results(results):
    l0 = True
    for fields in results:
        if not l0:
            print("---")
        print("url:", fields["url"])
        print("title:", fields["title"])
        print("type:", fields["type"])
        print("subtype:", fields["subtype"])
        print("org:", fields["org"])
        print("description:")
        print(_indented_text_block(fields["description"]))
        l0 = False


def _indented_text_block(s):
    return "\n".join(["  " + line for line in textwrap.wrap(s, 70)])


def _find(search_text):
    data = _init_data()
    parser = whoosh_parser.QueryParser("content", schema=data.schema)
    query = parser.parse(search_text)
    with data.searcher() as searcher:
        results = searcher.search(query, limit=None)
        _print_results(results)


def _build_index(args):
    http_sess = requests.Session()
    index_html = _get_index_html(http_sess)
    data = _init_data()
    existing_urls = _read_data_urls(data)
    indexed = 0
    event_ids = _index_event_ids(index_html)
    pbar = tqdm.tqdm(event_ids)
    for event_id in pbar:
        url = _event_url(event_id)
        if url in existing_urls:
            pbar.write("event %s exists, skipping" % event_id)
            continue
        pbar.write("adding event %s" % event_id)
        _index_event(url, data, http_sess, pbar)
        indexed += 1
        if args.max_events and indexed >= args.max_events:
            log.info("max events reached (%s), stopping", args.max_events)
            break


def _get_index_html(http_sess):
    html = _cached(CACHED_INDEX)
    if html:
        return html
    html = _http_get(http_sess, INDEX_URL)
    _cache(html, CACHED_INDEX)
    return html


def _http_get(http_sess, url):
    resp = http_sess.get(url, timeout=30)
    if resp.status_code != 200:
        raise SystemExit(
            "error reading '%s': %s (%s)" % (url, resp.reason, resp.status_code)
        )
    return resp.text.strip()


def _cached(path):
    try:
        f = open(path, "r")
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
        return None
    else:
        return f.read()


def _cache(s, path):
    _ensure_dir(os.path.dirname(path))
    with open(path, "w") as f:
        f.write(s)


def _ensure_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def _index_event_ids(s):
    return EVENT_ID_P.findall(s)


def _init_data():
    try:
        return _open_data_index()
    except whoosh_index.EmptyIndexError:
        return _create_data_index()


def _open_data_index():
    return whoosh_index.open_dir(DATA_DIR)


def _create_data_index():
    schema = whoosh_fields.Schema(
        url=whoosh_fields.ID(stored=True, unique=True),
        type=whoosh_fields.STORED(),
        title=whoosh_fields.STORED(),
        description=whoosh_fields.STORED(),
        org=whoosh_fields.STORED(),
        subtype=whoosh_fields.STORED(),
        content=whoosh_fields.TEXT(),
    )
    _ensure_dir(DATA_DIR)
    assert not whoosh_index.exists_in(DATA_DIR), DATA_DIR
    return whoosh_index.create_in(DATA_DIR, schema)


def _read_data_urls(ix):
    return set(ix.searcher().reader().field_terms("url"))


def _event_url(event_id):
    return EVENT_URL_PATTERN.format(event_id=event_id)


def _index_event(event_url, data, http_sess, pbar):
    with data.writer() as writer:
        event_html = _http_get(http_sess, event_url)
        writer.add_document(**_event_doc(event_url, event_html))
        for speaker_id in _speaker_ids(event_html):
            pbar.write("adding speaker %s" % speaker_id)
            speaker_url = _speaker_url(speaker_id)
            speaker_html = _http_get(http_sess, speaker_url)
            writer.add_document(**_speaker_doc(speaker_url, speaker_html))


def _event_doc(url, html):
    soup = bs4.BeautifulSoup(html, "html.parser")
    event_type = _event_type(soup)
    title = _event_title(soup)
    abstract = _event_abstract(soup)
    content = "\n".join(["event", event_type, title, abstract])
    return dict(
        url=url,
        type="event",
        title=title,
        description=abstract,
        subtype=event_type,
        org="",
        content=content,
    )


def _event_title(soup):
    title = soup.find("div", class_="maincardBody")
    if not title:
        return ""
    return title.text.strip()


def _event_abstract(soup):
    abstract = soup.find("div", class_="abstractContainer")
    if not abstract:
        return ""
    return abstract.text.strip()


def _event_type(soup):
    type = soup.find("div", class_="pull-right maincardHeader maincardType")
    if not type:
        return ""
    return type.text.strip()


def _speaker_ids(s):
    return SPEAKER_ID_P.findall(s)


def _speaker_url(speaker_id):
    return SPEAKER_URL_PATTERN.format(speaker_id=speaker_id)


def _speaker_doc(url, html):
    soup = bs4.BeautifulSoup(html, "html.parser")
    name = _speaker_name(soup)
    org = _speaker_org(soup)
    bio = _speaker_bio(soup)
    content = "\n".join(["speaker", name, org, bio])
    return dict(
        url=url,
        type="speaker",
        subtype="",
        title=name,
        description=bio,
        org=org,
        content=content,
    )


def _speaker_name(soup):
    name = soup.find("h3")
    if not name:
        return ""
    return name.text.strip()


def _speaker_bio(soup):
    name = soup.find("h3")
    if not name:
        return ""
    bio = name.find_next_sibling("div")
    if not bio:
        return ""
    return bio.text.strip()


def _speaker_org(soup):
    org = soup.find("h4")
    if not org:
        return ""
    return org.text.strip()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
