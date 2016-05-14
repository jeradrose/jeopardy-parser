#!/usr/bin/env python -OO
# -*- coding: utf-8 -*-

from __future__ import with_statement
from bs4 import BeautifulSoup
from glob import glob
from urllib.parse import urlparse
from urllib.parse import parse_qs

import argparse
import os
import re
import sqlite3
import sys

def main(args):
    """Loop thru all the games and parse them."""
    if not os.path.isdir(args.dir):
        print("The specified folder is not a directory.")
        sys.exit(1)
    NUMBER_OF_FILES = len(os.listdir(args.dir))
    if args.num_of_files:
        NUMBER_OF_FILES = args.num_of_files
    print("Parsing", NUMBER_OF_FILES, "files")
    sql = None
    with sqlite3.connect(args.database) as sql:
        if not args.stdout:
            sql.execute("""PRAGMA writable_schema = 1;""")
            sql.execute("""DELETE FROM sqlite_master WHERE type IN ('table', 'index', 'trigger')""")
            sql.execute("""PRAGMA writable_schema = 0;""")
            sql.execute("""VACUUM;""")
            sql.execute("""PRAGMA foreign_keys = ON;""")
            sql.execute("""CREATE TABLE games(
                id INTEGER PRIMARY KEY,
                airnumber INTEGER,
                airdate TEXT
            );""")
            sql.execute("""CREATE TABLE categories(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT UNIQUE
            );""")
            sql.execute("""CREATE TABLE players(
                id INTEGER PRIMARY KEY,
                name TEXT,
                occupation TEXT,
                location TEXT,
                is_originally INTEGER
            );""")
            sql.execute("""CREATE TABLE game_players(
                game_id INTEGER,
                player_id INTEGER,
                place INTEGER,
                first_break_score INTEGER,
                first_round_score INTEGER,
                second_round_score INTEGER,
                final_score INTEGER,
                coryat_score INTEGER
            );""")
            sql.execute("""CREATE TABLE clues(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER,
                round INTEGER,
                value INTEGER,
                category_id INTEGER,
                clue TEXT,
                answer TEXT,
                FOREIGN KEY(game_id) REFERENCES games(id),
                FOREIGN KEY(category_id) REFERENCES categories(id)
            );""")
        for i, file_name in enumerate(glob(os.path.join(args.dir, "*.html")), 1):
            print(file_name)
            with open(os.path.abspath(file_name)) as f:
                parse_game(f, sql, i)
        if not args.stdout:
            sql.commit()
        print("All done")

def parse_game(f, sql, gid):
    """Parses an entire Jeopardy! game and extract individual clues."""
    bsoup = BeautifulSoup(f, "lxml")

    scores_table_cb = bsoup.find("h3", text=re.compile("Scores at the first commercial break*.")).next_sibling.next_sibling

    # Get all player nickames
    player_nicknames = []
    for i in range(3):
        player_nicknames.append(scores_table_cb.find_all("td", class_="score_player_nickname")[i].get_text())

    # Get all player names
    player_names = []
    for i in range(3):
        player_names.append(bsoup.find_all("p", class_="contestants")[i].find("a").get_text())
        
    # Try to exact match player first names to nicknames
    player_names_to_nicknames = {}
    for i in reversed(range(3)):
        nickname = player_names[i].split()[0]
        if nickname in player_nicknames:
            name = player_names[i]
            player_names_to_nicknames[name] = nickname
            player_names.remove(name)
            player_nicknames.remove(nickname)

    # Do hacky matching to figure out remaining name/nicknames string trying to match from beginning of string
    for i in reversed(range(len(player_names))):
        name = player_names[i]
        for i in range(len(name)):
            matching_nickname = ""
            matches_found = 0
            for nickname in player_nicknames:
                if name[i:i+1] == nickname[i:i+1]:
                    matching_nickname = nickname
                    matches_found += 1
            if matches_found == 1:
                player_names_to_nicknames[name] = matching_nickname
                player_names.remove(name)
                player_nicknames.remove(matching_nickname)
                break
    
    if len(player_names) > 0:
        print("could not match all names to nicknames")
        return

    player_scores = {}
    player_names = scores_table_cb.find_all("td", class_="score_player_nickname")

    scores_table_j = bsoup.find("h3", text=re.compile("Scores at the end of the Jeopardy! Round:")).next_sibling.next_sibling
    scores_table_dj = bsoup.find("h3", text=re.compile("Scores at the end of the Double Jeopardy! Round:")).next_sibling.next_sibling
    scores_table_fs = bsoup.find("h3", text=re.compile("Final scores:")).next_sibling.next_sibling
    scores_table_cs = bsoup.find("a", text="Coryat scores").parent.next_sibling.next_sibling
    
    for i in range(3):
        player_scores[scores_table_cb.find_all("td", class_="score_player_nickname")[i].get_text()] = [
            int(scores_table_cb.find_all("td", class_=re.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")),
            int(scores_table_j.find_all("td", class_=re.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")),
            int(scores_table_dj.find_all("td", class_=re.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")),
            int(scores_table_fs.find_all("td", class_=re.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")),
            int(scores_table_cs.find_all("td", class_=re.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", ""))
        ]

    for i in range(3):
        p = bsoup.find_all("p", class_="contestants")[i]
        name = p.find("a").get_text()
        url = urlparse(p.find("a")["href"])
        params = parse_qs(url.query)
        if "player_id" in params:
            p_id = params["player_id"][0]
        m = re.search("^,(?: an?)? (.*?) (?:(originally) from|from) (.*?)(?: \(.*\))?$", p.contents[1])
        occupation = m.group(1)
        is_originally = m.group(2) != None
        location = m.group(3)
        nickname = name.split()[0]
        scores = player_scores[player_names_to_nicknames[name]]

        
        sql.execute("INSERT OR IGNORE INTO players(id, name, occupation, location, is_originally) VALUES(?, ?, ?, ?, ?);", (p_id, name, occupation, location, is_originally, ))
        sql.execute("INSERT INTO game_players(game_id, player_id, place, first_break_score, first_round_score, second_round_score, final_score, coryat_score) VALUES(?, ?, ?, ?, ?, ?, ?, ?);", (gid, p_id, 0, scores[0], scores[1], scores[2], scores[3], scores[4]))
        
    # The title is in the format: `J! Archive - Show #XXXX, aired 2004-09-16`,
    # where the last part is all that is required
    title_parts = bsoup.title.get_text().split()
    airdate = title_parts[-1]
    game_number = title_parts[-3].replace("#", "").replace(",", "")
    if not parse_round(bsoup, sql, 1, gid, game_number, airdate) or not parse_round(bsoup, sql, 2, gid, game_number, airdate):
        # One of the rounds does not exist
        pass
    # The final Jeopardy! round
    r = bsoup.find("table", class_="final_round")
    if not r:
        # This game does not have a final clue
        return
    category = r.find("td", class_="category_name").get_text()
    text = r.find("td", class_="clue_text").get_text()
    answer = BeautifulSoup(r.find("div", onmouseover=True).get("onmouseover"), "lxml")
    answer = answer.find("em").get_text()
    # False indicates no preset value for a clue
    insert(sql, [gid, airdate, 3, category, False, text, answer, game_number])


def parse_round(bsoup, sql, rnd, gid, game_number, airdate):
    """Parses and inserts the list of clues from a whole round."""
    round_id = "jeopardy_round" if rnd == 1 else "double_jeopardy_round"
    r = bsoup.find(id=round_id)
    # The game may not have all the rounds
    if not r:
        return False
    # The list of categories for this round
    categories = [c.get_text() for c in r.find_all("td", class_="category_name")]
    # The x_coord determines which category a clue is in
    # because the categories come before the clues, we will
    # have to match them up with the clues later on.
    x = 0
    for a in r.find_all("td", class_="clue"):
        is_missing = True if not a.get_text().strip() else False
        if not is_missing:
            value = a.find("td", class_=re.compile("clue_value")).get_text().lstrip("D: $")
            text = a.find("td", class_="clue_text").get_text()
            answer = BeautifulSoup(a.find("div", onmouseover=True).get("onmouseover"), "lxml")
            answer = answer.find("em", class_="correct_response").get_text()
            insert(sql, [gid, airdate, rnd, categories[x], value, text, answer, game_number])
        # Always update x, even if we skip
        # a clue, as this keeps things in order. there
        # are 6 categories, so once we reach the end,
        # loop back to the beginning category.
        #
        # Using modulus is slower, e.g.:
        #
        # x += 1
        # x %= 6
        #
        x = 0 if x == 5 else x + 1
    return True


def insert(sql, clue):
    """Inserts the given clue into the database."""
    # Clue is [game, airdate, round, category, value, clue, answer]
    # Note that at this point, clue[4] is False if round is 3
    if "\\\'" in clue[6]:
        clue[6] = clue[6].replace("\\\'", "'")
    if "\\\"" in clue[6]:
        clue[6] = clue[6].replace("\\\"", "\"")
    if not sql:
        print(clue)
        return
    sql.execute(
        "INSERT OR IGNORE INTO games VALUES(?, ?, ?);",
        (clue[0], clue[7], clue[1], )
    )
    sql.execute("INSERT OR IGNORE INTO categories(category) VALUES(?);", (clue[3], ))
    category_id = sql.execute("SELECT id FROM categories WHERE category=?;", (clue[3], )).fetchone()[0]
    sql.execute("INSERT INTO clues(game_id, round, value, category_id, clue, answer) VALUES(?, ?, ?, ?, ?, ?);", (clue[0], clue[2], clue[4], category_id, clue[5], clue[6], ))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse games from the J! Archive website.", add_help=False,
        usage="%(prog)s [options]")
    parser.add_argument("-d", "--dir", dest="dir", metavar="<folder>",
                        help="the directory containing the game files",
                        default="j-archive")
    parser.add_argument("-n", "--number-of-files", dest="num_of_files",
                        metavar="<number>", help="the number of files to parse",
                        type=int)
    parser.add_argument("-f", "--filename", dest="database",
                        metavar="<filename>",
                        help="the filename for the SQLite database",
                        default="clues.db")
    parser.add_argument("--stdout",
                        help="output the clues to stdout and not a database",
                        action="store_true")
    parser.add_argument("--help", action="help",
                        help="show this help message and exit")
    parser.add_argument("--version", action="version", version="2014.09.14")
    main(parser.parse_args())
