import csv
import logging
import os
import pathlib
import sqlite3
import sys
import threading
import typing
import urllib.parse
import urllib.request
from collections import namedtuple

import bs4
import pygsheets
import requests
import requests_cache
import werkzeug.http
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

SAVE_DIR = os.getenv("SAVE_DIR")
BOOKS_DB = os.getenv("BOOKS_DB")
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
THREAD_COUNT = int(os.getenv("THREAD_COUNT"))
LOG_FILENAME = os.getenv("LOG_FILENAME")
API_URL = "https://www.googleapis.com/books/v1/volumes?q=intitle:"
PD_URL = "https://www.pdfdrive.com/search?q="

client = pygsheets.authorize()

BookEntry = namedtuple(
    "BookEntry",
    [
        "name",
        "author",
        "downloaded",
        "type",
        "genre",
        "sub_genre",
        "topic",
        "link",
        "read",
        "download_status_cell",
    ],
)

file_handler = logging.FileHandler(filename=LOG_FILENAME, encoding="utf-8")
stdout_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    handlers=[file_handler, stdout_handler]
)

logger = logging.getLogger(__name__)

try:
    def create_downloaded_table():
        with sqlite3.connect(BOOKS_DB) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "create table downloaded (book_id text, link text, filepath text)"
            )

    create_downloaded_table()
except Exception as e:
    try:
        logger.info("Table 'downloaded' already exists.".encode())
    except:
        pass


def get_book_from_google_books(query, session):
    query = urllib.parse.quote(query)
    with session.get(f"{API_URL}{query}") as response:
        if not response.ok:
            return

        data = response.json()

        if "totalItems" not in data.keys():
            return
        if data["totalItems"] == 0:
            return

        try:
            isbn = [
                i
                for i in data["items"][0]["volumeInfo"]["industryIdentifiers"]
                if i["type"] == "ISBN_13"
            ]
        except Exception as e:
            return

        if len(isbn) < 1:
            isbn = [
                i
                for i in data["items"][0]["volumeInfo"]["industryIdentifiers"]
                if i["type"] == "ISBN_10"
            ]
        else:
            isbn = isbn[0]["identifier"]

        if len(isbn) == 0:
            return

        return {
            "data": data,
            "isbn": isbn,
        }


class Book:
    def __init__(self, _id):
        self.direct_download_links = []
        self.session = requests_cache.CachedSession()
        self.filepath = None
        self.id = _id

    def record_download(self, download_link):
        if self.filepath is None:
            return

        with sqlite3.connect(BOOKS_DB) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "insert into downloaded values (?, ?, ?)",
                (self.id, download_link, self.filepath),
            )

    @property
    def is_downloaded(self) -> bool:
        with sqlite3.connect(BOOKS_DB) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "select filepath from downloaded where book_id=?", (self.id,)
            )
            download_entry = cursor.fetchone()
            if download_entry is not None:
                if os.path.isfile(download_entry[0]):
                    return True
            return False

    @staticmethod
    def get_filename_from_response(response, download_link) -> str:
        if "Content-Disposition" in response.headers.keys():
            filename = werkzeug.http.parse_options_header(
                response.headers["Content-Disposition"]
            )[1]["filename"]
            filename = urllib.parse.unquote(filename)
        else:
            filename = urllib.parse.unquote(download_link.split("/").pop())

        if "download.pdf?id=" in filename:
            return ""
        return filename.replace("?", "")

    def download_file(self):
        ...


class LibgenBook(Book):
    download_links = ("http://library.lol/main/",)

    def __init__(self, _id, libgen_id, db_cursor):
        super().__init__(_id)
        self.libgen_id = libgen_id
        self.md5 = None

    def _get_md5(self) -> str:
        with self.session.get(
            f"https://www.libgen.is/json.php?ids={self.libgen_id}&fields=md5"
        ) as response:
            if not response.ok:
                return

            data = response.json()
            self.md5 = data[0]["md5"]
            return self.md5

    def _get_download_links(self):
        for link in LibgenBook.download_links:
            with self.session.get(f"{link}{self.md5}") as response:
                if not response.ok:
                    continue

                soup = bs4.BeautifulSoup(response.text, "html.parser")
                self.direct_download_links = [
                    i.attrs["href"] for i in soup.find(id="download").find_all("a")
                ]

    def _download(self, save_dir=SAVE_DIR):
        for download_link in self.direct_download_links:
            with self.session.head(
                download_link, allow_redirects=True
            ) as head_response:
                if not head_response.ok:
                    continue

                self.filepath = LibgenBook.get_filename_from_response(
                    head_response, download_link
                )
                self.filepath = str(pathlib.Path(save_dir) / self.filepath)

                if os.path.isfile(self.filepath):
                    with sqlite3.connect(BOOKS_DB) as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "select filepath from downloaded where book_id=?",
                            (self.id,),
                        )
                        download_entry = cursor.fetchone()
                        if download_entry is None:
                            self.record_download(download_link)
                    return

            with self.session.get(download_link) as response:
                if not response.ok:
                    continue

                try:
                    with open(self.filepath, "wb") as f:
                        f.write(response.content)
                        logger.info(f"Item with id ({self.id}) saved to ({self.filepath}).")
                        self.record_download(download_link)
                        return
                except Exception as e:
                    logger.error(e)

        logger.error(f"Downloading item with id ({self.id}) unsuccessful.")

    def download_file(self, save_dir=SAVE_DIR):
        if self.is_downloaded:
            return

        self._get_md5()
        if self.md5 is None:
            return

        self._get_download_links()
        self._download(save_dir)


class PdfDriveBook(Book):
    def __init__(self, _id, url, db_cursor):
        super().__init__(_id)
        self.url = f"https://www.pdfdrive.com{url}"
        self.session = requests.session()

    @staticmethod
    def _build_pdfdrive_download_link(data_id, session_id):
        return f"https://www.pdfdrive.com/download.pdf?id={data_id}&h={session_id}"

    def _get_download_links(self) -> typing.List[str]:
        with self.session.get(self.url) as response:
            soup = bs4.BeautifulSoup(response.text, "html.parser")

            try:
                data_id = soup.find(id="previewButtonMain").attrs["data-id"]
                sess_id = (
                    soup.find(id="previewButtonMain")
                    .attrs["data-preview"]
                    .split("session=")[1]
                )
            except AttributeError:
                return

            self.direct_download_links = [
                PdfDriveBook._build_pdfdrive_download_link(data_id, sess_id)
            ]
            return self.direct_download_links

    def _download(self, save_dir=SAVE_DIR):
        if len(self.direct_download_links) == 0:
            return

        with self.session.head(
            self.direct_download_links[0], allow_redirects=True
        ) as head_response:
            if not head_response.ok:
                return

            self.filepath = (
                PdfDriveBook.get_filename_from_response(
                    head_response, self.direct_download_links[0]
                )
                .replace(":", " - ")
                .replace("  ", " ")
            )

            if self.filepath == "":
                return

            self.filepath = str(
                pathlib.Path(save_dir) / self.filepath.replace(":", " - ")
            ).replace("  ", " ")

            if os.path.isfile(self.filepath):
                logger.info(f"File ({self.filepath}) already exists.")
                with sqlite3.connect(BOOKS_DB) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "select filepath from downloaded where book_id=?", (self.id,)
                    )
                    download_entry = cursor.fetchone()
                    if download_entry is None:
                        self.record_download(self.direct_download_links[0])
                return

        with self.session.get(self.direct_download_links[0]) as response:
            try:
                with open(self.filepath, "wb") as f:
                    f.write(response.content)
                    logger.info(f"Item with id ({self.id}) saved to ({self.filepath}).".encode("utf-8"))
                    self.record_download(self.direct_download_links[0])
            except Exception as e:
                logger.error(e)
                return

    def download_file(self, save_dir=SAVE_DIR):
        self._get_download_links()
        self._download(save_dir)


class ZlibBook(Book):
    def __init__(self, _id, url):
        super().__init__(_id)
        self.url = url
        self.direct_download_links = []

    def _get_download_links(self):
        with self.session.get(f"https://za1lib.org{self.url}") as response:
            soup = BeautifulSoup(response.text, "html.parser")
            self.direct_download_links = [
                "https://za1lib.org"
                + soup.find("a", class_="addDownloadedBook").attrs["href"]
            ]

    def _download(self, save_dir=SAVE_DIR):
        if len(self.direct_download_links) == 0:
            return

        with self.session.head(self.direct_download_links[0]) as head_response:
            if not head_response.ok:
                return

            self.filepath = (
                ZlibBook.get_filename_from_response(
                    head_response, self.direct_download_links[0]
                )
                .replace(":", " - ")
                .replace("  ", " ")
            )
            self.filepath = str(
                pathlib.Path(save_dir) / self.filepath.replace(":", " - ")
            ).replace("  ", " ")

            if os.path.isfile(self.filepath):
                with sqlite3.connect(BOOKS_DB) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "select filepath from downloaded where book_id=?", (self.id,)
                    )
                    download_entry = cursor.fetchone()
                    if download_entry is None:
                        self.record_download(self.direct_download_links[0])
                return

        with self.session.get(self.direct_download_links[0]) as response:
            try:
                with open(self.filepath, "wb") as f:
                    f.write(response.content)
                    logger.info(f"Item with id ({self.id}) saved to ({self.filepath}).")
                    self.record_download(self.direct_download_links[0])
            except Exception as e:
                logger.error(e)
                return

    def download_file(self):
        self._get_download_links()
        self._download()


class BookDownloader:
    def __init__(self, download_list):
        self.download_list = download_list
        self.con = None
        self.cur = None
        self.t = threading.Thread(target=self.run)
        self.t.start()

    def run(self):
        for book, book_id, session in self.download_list:
            self.download_book(book, book_id, session)

    def download_book(self, book, book_id, session):
        book_name = book.name
        author = book.author

        book = get_book_from_google_books(book.name + " " + book.author, session)
        if book is None:
            return

        book["id"] = book_id
        book["name"] = book_name
        book["author"] = author

        libgen_attempt = self.try_libgen(book, session)
        if libgen_attempt:
            return

        # zlib has 5 download limit per IP per 24 hours, essentially useless
        zlib_attempt = False
        if zlib_attempt:
            return

        pdfdrive_attempt = self.try_pdfdrive(book, session)
        if pdfdrive_attempt:
            return

    def try_libgen(self, book, session):
        try:
            with session.get(f"http://libgen.is/json.php?isbn={book['isbn']}&fields=ID") as response:
                if not response.ok:
                    return False

                ids = [i["id"] for i in response.json()]

        except Exception as e:
            logging.error(e)
            return False

        if len(ids) > 0:
            LibgenBook(book["id"], ids[0], self.con).download_file()
            return True

        return False

    def try_zlib(self, book, session):
        query = urllib.parse.quote(f"{book['name']} {book['author']}")
        with session.get(f"https://za1lib.org/s/{query}") as response:
            soup = BeautifulSoup(response.text, "html.parser")
            results = [
                (
                    i.find("h3").find("a").contents,
                    i.find("h3").find("a")["href"],
                    [
                        j.contents[0]
                        for j in i.find("div", class_="authors")
                        if isinstance(j, bs4.Tag)
                    ],
                    i.find("div", class_="checkBookDownloaded").attrs["data-isbn"],
                )
                for i in soup.find_all("tr", class_="bookRow")
            ]

            for r in results:
                if len(r[2]) > 0:
                    author = r[2][0]
                query = f"{r[0]} {author}"

                if len(r[3]) > 0:
                    if r[3] == book["isbn"]:
                        ZlibBook(book["id"], r[1]).download_file()
                        return True

                g_books_results = get_book_from_google_books(query, session)
                if g_books_results is None:
                    continue

                if g_books_results["isbn"] == book["isbn"]:
                    ZlibBook(book["id"], r[1]).download_file()
                    return True

    def try_pdfdrive(self, book, session):
        query = urllib.parse.quote(f"{book['name']} {book['author']}")
        for link in (f"{PD_URL}{query}&em=1", f"{PD_URL}{query}"):
            with session.get(link) as response:
                soup = BeautifulSoup(response.text, "html.parser")
                results = soup.find_all("div", class_="file-left")

                try:
                    res = [
                        (
                            r.contents[1].contents[1].attrs["title"],
                            r.contents[1].attrs["href"],
                        )
                        for r in results
                    ]
                except Exception as e:
                    return False

                for r in results:
                    title = r.contents[1].contents[1].attrs["title"]

                    if link == f"{PD_URL}{query}&em=1":
                        PdfDriveBook(
                            book["id"], r.contents[1].attrs["href"], self.con
                        ).download_file()
                        return True

                    g_books_results = get_book_from_google_books(title, session)
                    if g_books_results is None:
                        continue

                    if g_books_results["isbn"] == book["isbn"]:
                        PdfDriveBook(
                            book["id"], r.contents[1].attrs["href"], self.con
                        ).download_file()
                        return True
        return False


def main():
    sh = None
    if not os.path.isfile("books.tsv"):
        sh = client.open_by_url(SPREADSHEET_URL)
        sh.export(file_format="tsv", filename="books.tsv")

    download_list = []
    with requests_cache.CachedSession() as session:
        with open("books.tsv", encoding="utf-8") as tsv:
            tsv_file = csv.reader(tsv, delimiter="\t")
            for i, row in enumerate(tsv_file):
                row = row[:9]

                if i == 0 or row[0] == "":
                    continue

                books_in_row = (
                    [j.strip() for j in row[0].split(",")]
                    if "," in row[0]
                    else [row[0]]
                )

                for b in books_in_row:
                    r = [b, *row[1:]]
                    book = BookEntry(*r, f"C{i + 1}")

                    if book.downloaded != "Y":
                        download_list.append(
                            (book, f"{i}{book.name}{book.author}", session)
                        )

        logger.info(f"Attempting to download {len(download_list)} books.")

        """
        Splits the download list into smaller pieces, then creates and runs a thread for each piece.
        The number of pieces the download list is split into is equal to the value THREAD_COUNT is
        set to.
        """
        for i in range(THREAD_COUNT):
            BookDownloader(download_list[i::THREAD_COUNT])


if __name__ == "__main__":
    main()
