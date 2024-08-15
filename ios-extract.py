#!/usr/bin/env python3

import coloredlogs
import datetime
import logging
import json
import os
import plistlib
import sqlite3
import shutil
import re
import phonenumbers

from pprint import pprint

coloredlogs.install(level=logging.INFO)

PHOTOS_BACKUP_IDS = {}

PHOTOS_INTERESTING_EXTENSIONS = [
    "HEIC",
    "JPG",
    "JPEG",
    "PNG",
    "GIF",
    "MOV",
    "MP4",
]

BACKUP_PHOTOS_ROOT = os.environ["BACKUP_PHOTOS_ROOT"]


class InvalidBackupException(Exception):
    pass


class ApplicationNotFoundException(Exception):
    pass


class FileNotFoundException(Exception):
    pass


class ContactsNotDefinedException(Exception):
    pass


CONTACTS_LOOKUP = {}


def parse_phone_number(input):
    try:
        parsed = phonenumbers.parse(input, "US")
        return f"+{parsed.country_code}{parsed.national_number}"
    except phonenumbers.phonenumberutil.NumberParseException:
        return


def simple_query(db, query, args=None, filename=None, by=None):
    logging.info(f"simple_query({db}, {query}, {args=}, {filename=}, {by=})")
    cursor = db.cursor()
    cursor.execute(query, args or [])

    result = [dict(row) for row in cursor]

    if by is not None:
        result = {row[by]: row for row in result}

    if filename:
        to_json(result, filename)

    return result


def simple_queries(db, *args):
    for filename, query in args:
        simple_query(db, query, filename=filename)


def to_json(data, filename):
    logging.info(f"Writing json to {filename}")

    if "/" in filename:
        os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w") as f:
        if isinstance(data, list):
            for row in data:
                json.dump(row, f, default=str)
                f.write("\n")
        else:
            json.dump(data, f, default=str, indent=2)


def make_temporary_copy(id, src_path, dst_path=None):
    dst_path = dst_path or src_path
    tmp_path = f"/tmp/{id}/{dst_path}"

    if os.path.exists(tmp_path):
        logging.info(f"temporary file already exists: {tmp_path}")
    else:
        os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
        shutil.copy2(src_path, tmp_path)
        logging.info(f"temporary file created: {tmp_path} (original: {src_path})")

    return tmp_path


class IOSBackup:
    def __init__(self, path):
        self.path = path

        if not os.path.exists(os.path.join(self.path, "Info.plist")):
            raise InvalidBackupException(f"{path} does not contain an Info.plist")

        with open(os.path.join(self.path, "Info.plist"), "rb") as f:
            self.info_plist = plistlib.load(f)

        self.id = self["Unique Identifier"]
        self.manifest_temp_path = make_temporary_copy(
            self.id, os.path.join(self.path, "Manifest.db"), "Manifest.db"
        )

        self.db = sqlite3.connect(self.manifest_temp_path)
        self.db.row_factory = sqlite3.Row

        logging.info(f"Backup ready: {self}")

    def __getitem__(self, key):
        if key in self.info_plist:
            return self.info_plist[key]

    def app(self, id):
        if id not in self["Applications"]:
            raise ApplicationNotFoundException(f"{id} not found in {self}")

        return IOSBackupApplication(self, id)

    def file(self, domain, path):
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT fileID FROM Files WHERE domain = ? AND relativePath = ?",
            [domain, path],
        )
        results = list(cursor)

        if len(results) != 1:
            raise FileNotFoundException(f"File {path} not found in domain {domain}")

        return IOSBackupFile(
            self,
            domain,
            path,
            id=results[0]["fileID"],
            application=None,
        )

    def files(self, domain=None, path=None):
        cursor = self.db.cursor()

        where = []
        args = []

        if domain:
            args.append(domain)
            if "%" in domain:
                where.append("domain like ?")
            else:
                where.append("domain = ?")

        if path:
            args.append(path)
            if "%" in path:
                where.append("relativePath like ?")
            else:
                where.append("relativePath = ?")

        sql = "SELECT * FROM Files WHERE " + " AND ".join(where)

        cursor.execute(sql, args)
        for row in cursor:
            yield IOSBackupFile(
                self,
                row["domain"],
                row["relativePath"],
                id=row["fileID"],
                application=None,
            )

    def __repr__(self):
        return "IOSBackup<{}>".format(
            json.dumps(
                {
                    "path": self.path,
                    "name": self["Display Name"],
                    "guid": self["GUID"],
                    "date": self["Last Backup Date"],
                    "number": self["Phone Number"],
                    "hardware": self["Product Type"],
                    "software": self["Product Version"],
                },
                indent=2,
                default=str,
            )
        )


class IOSBackupApplication:
    def __init__(self, backup, id):
        self.backup = backup
        self.id = id
        self.domain = f"AppDomain-{self.id}"

        self.files = {}
        cursor = self.backup.db.cursor()
        cursor.execute(
            "SELECT fileID, relativePath, flags, file FROM Files WHERE domain = ?",
            [self.domain],
        )
        for row in cursor:
            self.files[row["relativePath"]] = IOSBackupFile(
                self.backup,
                domain=self.domain,
                path=row["relativePath"],
                id=row["fileID"],
                application=self,
            )

    def __repr__(self):
        return "IOSBackup<{}>".format(
            json.dumps(
                {
                    "id": self.id,
                },
                indent=2,
                default=str,
            )
        )


class IOSBackupFile:
    def __init__(self, backup, domain, path, id=None, application=None):
        self.backup = backup
        self.domain = domain  # todo: default
        self.path = path
        self.id = id  # todo: default
        self._metadata = None

        self.real_path = os.path.join(self.backup.path, self.id[:2], self.id)
        self.temp_path = None

    def metadata(self):
        if not self._metadata:
            cursor = self.backup.db.cursor()
            cursor.execute("SELECT file FROM Files WHERE fileID = ?", [self.id])
            results = list(cursor)
            if len(results) != 1:
                raise FileNotFoundException(f"Could not load metadata for {self}")

            self._metadata = plistlib.loads(results[0]["file"])

        return self._metadata

    def open(self, mode):
        return open(self.real_path, mode)

    def open_db(self):
        self.temp_path = make_temporary_copy(
            f"{self.backup.id}/{self.domain}",
            self.real_path,
            self.path,
        )
        db = sqlite3.connect(self.temp_path)
        db.row_factory = sqlite3.Row

        return db

    def __repr__(self):
        return "IOSBackup<{}>".format(
            json.dumps(
                {
                    "domain": self.domain,
                    "path": self.path,
                    "id": self.id,
                },
                indent=2,
                default=str,
            )
        )


def backup_bgstats():
    app = backup.app("nl.vissering.BoardGameStats")

    db = app.files["Documents/Model.sqlite"].open_db()
    logging.info("Running simple backups")
    simple_queries(
        db,
        (
            "BGStats/locations.json",
            "select Z_PK as id, ZUUID as uuid, ZNAME as name from ZLOCATION",
        ),
        (
            "BGStats/games.json",
            "select Z_PK as id, ZUUID as uuid, ZBGGID as bgg_id, ZNAME as name from ZGAME",
        ),
        (
            "BGStats/players.json",
            "select Z_PK as id, ZUUID as uuid, ZNAME as name from ZPLAYER",
        ),
        # Raw play data, plays-full is this all combined
        (
            "BGStats/plays.json",
            'select Z_PK as id, ZUUID as uuid, ZPLAYDATETIME as date, ZPLAYEDGAME as game_id, ZPLAYLOCATION as location_id, trim((coalesce(ZBOARD, "") || "／" || coalesce(ZCOMMENTS, "")), "／") as comments from ZPLAY',
        ),
        (
            "BGStats/scores.json",
            "select Z_PK as id, ZPLAY as play_id, ZPLAYER as player_id, ZWIN as winner, ZSCORE as score, ZTEAM as team from ZPLAYERSCORE",
        ),
        (
            "BGStats/plays-expansion.json",
            "select Z_PK as id, ZPLAY as play_id, ZEXPANSIONGAME as game_id from ZEXPANSIONPLAY",
        ),
    )

    logging.info("Backing up full play data")
    plays_full = simple_query(
        db,
        """
        select
            play.Z_PK as id,
            play.ZUUID as uuid,
            play.ZPLAYDATETIME as date,
            game.ZNAME as game,
            location.ZNAME as location,
            trim((
                coalesce(play.ZBOARD, "")
                || "／"
                || coalesce(play.ZCOMMENTS, "")
            ), "／") as comments,
            group_concat(expansion_game.ZNAME, "／") as expansions
        from
            ZPLAY as play
            LEFT JOIN ZGAME as game ON play.ZPLAYEDGAME = game.Z_PK
            LEFT JOIN ZLOCATION as location ON play.ZPLAYLOCATION = location.Z_PK
            LEFT JOIN ZEXPANSIONPLAY as expansion ON play.Z_PK = expansion.ZPLAY
            LEFT JOIN ZGAME as expansion_game ON expansion.ZEXPANSIONGAME = expansion_game.Z_PK
        group by 
            id
    """,
    )

    for play in plays_full:
        play["date"] = datetime.date(2001, 1, 1) + datetime.timedelta(
            seconds=play["date"]
        )

    plays_full = {play["id"]: play for play in plays_full}

    players_full = simple_query(
        db,
        """
        select
            score.ZPLAY as play_id,
            player.ZNAME as name,
            score.ZWIN as winner,
            score.ZSCORE as score,
            score.ZTEAM as team
        from
            ZPLAYERSCORE as score
            LEFT JOIN ZPLAYER as player on score.ZPLAYER = player.Z_PK
    """,
    )

    for player in players_full:
        play_id = player["play_id"]
        del player["play_id"]

        if not player["score"].isalnum():
            player["score"] = eval(player["score"] or "0")  # I know

        player["winner"] = bool(player["winner"])

        if player["team"] is None:
            del player["team"]

        plays_full[play_id].setdefault("players", []).append(player)

    to_json(list(plays_full.values()), "BGStats/plays-full.json")


def backup_contacts():
    contacts_db = backup.file(
        "HomeDomain", "Library/AddressBook/AddressBook.sqlitedb"
    ).open_db()

    people = simple_query(
        contacts_db,
        """
        select
            ROWID as id,
            GUID as uuid,
            First as first_name,
            Last as last_name,
            Nickname as nickname,
            Organization as organization
        from
            ABPerson
    """,
    )
    people = {person["id"]: person for person in people}

    for person in people.values():
        # Set display names
        display_name = []

        if person["first_name"]:
            display_name.append(person["first_name"])

        if person["nickname"]:
            display_name.append('"' + person["nickname"] + '"')

        if person["last_name"]:
            display_name.append(person["last_name"])

        if person["organization"]:
            if display_name:
                display_name.append("(" + person["organization"] + ")")
            else:
                display_name.append(person["organization"])

        person["display_name"] = " ".join(display_name)

        # Remove null values
        to_remove = {k for k, v in person.items() if v == None}
        for k in to_remove:
            del person[k]

    values = simple_query(
        contacts_db, "select record_id as person_id, value from ABMultiValue"
    )
    for row in values:
        person_id = row["person_id"]
        contact = row["value"]

        if not contact:
            continue

        if "@" in contact:
            type = "email"
        elif contact.isdigit() and len(contact) <= 6:
            type = "shortcode"
        elif phone := parse_phone_number(contact):
            contact = phone
            type = "phone"
        else:
            type = "unknown"

        people[person_id].setdefault("contacts", {}).setdefault(type, []).append(
            contact
        )

    to_json(list(people.values()), "messages/contacts.json")

    for person in people.values():
        for contacts in person["contacts"].values():
            for contact in contacts:
                CONTACTS_LOOKUP[contact] = (person["uuid"], person["display_name"])


def backup_messages():
    sms_db = backup.file("HomeDomain", "Library/SMS/sms.db").open_db()

    if not CONTACTS_LOOKUP:
        raise ContactsNotDefinedException("Contacts must be loaded before messages")

    chats = simple_query(sms_db, "select ROWID as id, guid from chat", by="id")

    handles = simple_query(
        sms_db,
        "select ROWID as id, id as contact, country, service from handle",
        by="id",
    )
    chat_handles = simple_query(
        sms_db, "select chat_id, handle_id from chat_handle_join"
    )

    for row in chat_handles:
        handle = handles[row["handle_id"]]

        contact = handle["contact"]
        if phone := parse_phone_number(contact):
            contact = phone

        if contact in CONTACTS_LOOKUP:
            (uuid, name) = CONTACTS_LOOKUP[contact]
            handle["uuid"] = uuid
            handle["name"] = name
        else:
            logging.warning(f"Unknown contact {contact}")

        chats[row["chat_id"]].setdefault("members", []).append(handle)

    to_json(list(chats.values()), "messages/chats.json")

    messages = simple_query(
        sms_db,
        "select ROWID as id, guid, date, handle_id, text, reply_to_guid from message",
        by="id",
    )
    chat_messages = simple_query(
        sms_db, "select chat_id, message_id from chat_message_join"
    )

    for row in messages.values():
        if not row["handle_id"] in handles:
            continue

        handle = handles[row["handle_id"]]

        contact = handle["contact"]
        if phone := parse_phone_number(contact):
            contact = phone

        if contact in CONTACTS_LOOKUP:
            (uuid, name) = CONTACTS_LOOKUP[contact]
            row["sender_uuid"] = uuid
            row["sender_name"] = name
        else:
            logging.warning(f"Unknown contact {contact}")

    for row in chat_messages:
        chats[row["chat_id"]].setdefault("messages", []).append(
            messages[row["message_id"]]
        )

    attachments = simple_query(
        sms_db,
        "select ROWID as id, guid, created_date as date, filename, uti, mime_type, transfer_name, total_bytes from attachment",
        by="id",
    )
    message_attachments = simple_query(
        sms_db, "select message_id, attachment_id from message_attachment_join"
    )

    for row in message_attachments:
        messages[row["message_id"]].setdefault("attachments", []).append(
            attachments[row["attachment_id"]]
        )

    to_json(list(chats.values()), "messages/chats-full.json")

    for chat in chats.values():
        try:
            to_json(chat["messages"], f'messages/message-data/{chat["id"]}.json')
        except KeyError:
            pass


def backup_photos():
    if backup.id not in PHOTOS_BACKUP_IDS:
        logging.critical(f"Please add {backup.id} to PHOTOS_BACKUP_IDS")
        exit(1)

    name = PHOTOS_BACKUP_IDS[backup.id]

    # Find most recent picture
    most_recent = None

    for dir, _, files in os.walk(BACKUP_PHOTOS_ROOT):
        for file in files:
            if file.startswith("."):
                continue

            path = os.path.join(dir, file)

            timestamp = os.path.getmtime(path) or os.stat(path).st_birthtime
            date = datetime.datetime.fromtimestamp(timestamp)

            if most_recent is None or date > most_recent:
                most_recent = date

    # Backup photos I took
    for i, photo in enumerate(backup.files("CameraRollDomain", "Media/DCIM/%")):
        date = datetime.datetime.fromtimestamp(
            photo.metadata()["$objects"][1]["LastModified"]
        )
        if most_recent is not None and date < most_recent:
            continue

        extension = photo.path.split(".")[-1].upper()
        if extension not in PHOTOS_INTERESTING_EXTENSIONS:
            logging.info(f"Skipping {photo.path} by extension")
            continue

        filename = photo.path.split("/")[-1]

        src_path = photo.real_path
        dst_path = f"{root}/{date.year}/{date.date()} {name} {filename}"
        print(f"{date}\n\tsrc: {src_path}\n\tdst: {dst_path}")

        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        shutil.copy2(src_path, dst_path)

    # TODO: Backup photos sent to me


backup_functions = [
    backup_bgstats,
    backup_contacts,
    backup_messages,
    backup_photos,
]

backup_roots = [
    "/Volumes/Backups/iPhone/Active/",
]

for backup_root in backup_roots:
    for backup_id in os.listdir(backup_root):
        try:
            backup = IOSBackup(os.path.join(backup_root, backup_id))
        except InvalidBackupException:
            logging.info(f"Skipping {backup_id}, not a backup")
            continue

        for backup_function in backup_functions:
            logging.info(f"Running {backup_function}")
            try:
                backup_function()
            except ApplicationNotFoundException:
                logging.warning("Game not found")
