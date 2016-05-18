#!/usr/bin/env python -OO
# -*- coding: utf-8 -*-

from __future__ import with_statement
from bs4 import BeautifulSoup
from glob import glob
from urllib.parse import urlparse
from urllib.parse import parse_qs
from scipy.stats import rankdata

import argparse
import os
import regex
import sqlite3
import sys

def main(args): 
    """Loop thru all the games and parse them."""
    if not os.path.isdir(args.dir):
        print("The specified folder is not a directory.")
        sys.exit(1)
    total_files = len(os.listdir(args.dir))
    if args.num_of_files:
        total_files = args.num_of_files
    print("Parsing", total_files, "files")
    sql = None
    with sqlite3.connect(args.database) as sql:
        if not args.stdout:
            sql.execute("""PRAGMA writable_schema = 1;""")
            sql.execute("""DELETE FROM sqlite_master WHERE type IN ('table', 'index', 'trigger')""")
            sql.execute("""PRAGMA writable_schema = 0;""")
            sql.execute("""VACUUM;""")
            sql.execute("""PRAGMA foreign_keys = ON;""")
            sql.execute("""CREATE TABLE games(
                game_id INTEGER PRIMARY KEY,
                airnumber INTEGER,
                airdate TEXT,
                game_data_complete INTEGER,
                notes TEXT
            );""")
            sql.execute("""CREATE TABLE categories(
                category_id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT UNIQUE
            );""")
            sql.execute("""CREATE TABLE players(
                player_id INTEGER PRIMARY KEY,
                name TEXT,
                nickname TEXT,
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
                coryat_score INTEGER,
                PRIMARY KEY(game_id, player_id),
                FOREIGN KEY(game_id) REFERENCES games(game_id),
                FOREIGN KEY(player_id) REFERENCES players(player_id)
            );""")
            sql.execute("""CREATE TABLE clues(
                clue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                game_id INTEGER,
                round INTEGER,
                value INTEGER,
                is_daily_double INTEGER,
                category_id INTEGER,
                order_number INTEGER,
                clue TEXT,
                answer TEXT,
                answer_player_id INTEGER,
                FOREIGN KEY(game_id) REFERENCES games(game_id),
                FOREIGN KEY(category_id) REFERENCES categories(category_id),
                FOREIGN KEY(answer_player_id) REFERENCES players(player_id)
            );""")
            sql.execute("""CREATE TABLE clue_wrong_answers(
                clue_id INTEGER,
                player_id INTEGER,
                answer TEXT,
                PRIMARY KEY(clue_id, player_id),
                FOREIGN KEY(clue_id) REFERENCES clues(clue_id),
                FOREIGN KEY(player_id) REFERENCES players(player_id)
            )""")
            
        file_numbers = []
        
        for file_name in glob(os.path.join(args.dir, "*.html")):
            file_numbers.append(int(file_name.replace(".html", "").replace(args.dir + "\\", "")))
            
        for file_number in sorted(file_numbers):
            print(file_number)
            with open(os.path.abspath(args.dir + "\\" + str(file_number) + ".html")) as f:
                parse_game(f, sql, file_number)
        if not args.stdout:
            sql.commit()
        print("All done")

def parse_game(f, sql, gid):
    """Parses an entire Jeopardy! game and extract individual clues."""
    bsoup = BeautifulSoup(f, "lxml")

    # The title is in the format: `J! Archive - Show #XXXX, aired 2004-09-16`,
    # where the last part is all that is required
    title_parts = bsoup.title.get_text().split()
    airdate = title_parts[-1]
    game_number = title_parts[-3].replace("#", "").replace(",", "")
    notes = bsoup.find("div", { "id": "game_comments" }).get_text()
    notes = None if notes == "" else notes

    sql.execute(
        "INSERT OR IGNORE INTO games VALUES(?, ?, ?, ?, ?);",
        (gid, game_number, airdate, 0, notes, )
    )
    player_data_complete = parse_players(bsoup, sql, gid)
    round_data_complete = parse_round(bsoup, sql, 1, gid, game_number, airdate) and parse_round(bsoup, sql, 2, gid, game_number, airdate)
    r = bsoup.find("table", class_="final_round")
    if not r:
        # This game does not have a final clue
        return
    category = r.find("td", class_="category_name").get_text()
    text = r.find("td", class_="clue_text").get_text()
    answer = BeautifulSoup(r.find("div", onmouseover=True).get("onmouseover"), "lxml")
    answer = answer.find("em").get_text()
    # False indicates no preset value for a clue
    insert(sql, [gid, airdate, 3, category, False, text, answer, game_number, None, None, 0])
    
    if player_data_complete and round_data_complete:
        sql.execute("UPDATE games SET game_data_complete = 1 WHERE game_id = ?", (gid,))

def parse_players(bsoup, sql, gid):
    player_ids = {}    
    
    contestants = bsoup.find_all("p", class_="contestants")
    
    total_players = len(contestants)
    
    if contestants:
        for i in range(total_players):
            p = contestants[i]
            for match in p.findAll('i'):
                match.unwrap()

    contestants = bsoup.find_all("p", class_="contestants")
    if contestants:
        for i in range(total_players):
            p = contestants[i]
            name = p.find("a").get_text()
            url = urlparse(p.find("a")["href"])
            params = parse_qs(url.query)
            if "player_id" in params:
                p_id = params["player_id"][0]
            m = regex.search("^,(?: an?)? (.*?) (?:(originally) from|from) (.*?)(?: \(.*\))?$", ''.join(p.contents[1:]))
            occupation = m.group(1)
            is_originally = m.group(2) != None
            location = m.group(3)
            player_ids[name] = p_id
            
            sql.execute("INSERT OR IGNORE INTO players(player_id, name, occupation, location, is_originally) VALUES(?, ?, ?, ?, ?);", (p_id, name, occupation, location, is_originally, ))
            sql.execute("INSERT OR IGNORE INTO game_players(game_id, player_id) VALUES(?, ?)", (gid, p_id,))
    
    st_h3_cb = bsoup.find("h3", text=regex.compile("Scores at the first commercial break*."))
    st_h3_j = bsoup.find("h3", text=regex.compile("Scores at the end of the Jeopardy! Round:"))
    st_h3_dj = bsoup.find("h3", text=regex.compile("Scores at the end of the Double Jeopardy! Round:"))
    st_h3_fs = bsoup.find("h3", text=regex.compile("Final scores:"))
    
    scores_table_cb = st_h3_cb.next_sibling.next_sibling if (st_h3_cb) else None
    scores_table_j = st_h3_j.next_sibling.next_sibling if (st_h3_j) else None
    scores_table_dj = st_h3_dj.next_sibling.next_sibling if (st_h3_dj) else None
    scores_table_fs = st_h3_fs.next_sibling.next_sibling if (st_h3_fs) else None
    st_h3_cs = bsoup.find("a", text="Coryat scores").parent if bsoup.find("a", text="Coryat scores") else None
    scores_table_cs = st_h3_cs.next_sibling.next_sibling if (st_h3_cs) else None
        
    if (scores_table_cb):
        # Get all player nickames
        player_nicknames = []
        for i in range(total_players):
            player_nicknames.append(scores_table_cb.find_all("td", class_="score_player_nickname")[i].get_text())
    
        # Get all player names
        player_names = []
        for i in range(total_players):
            player_names.append(bsoup.find_all("p", class_="contestants")[i].find("a").get_text())
            
        # Try to exact match player first names to nicknames
        player_nicknames_to_names = {}
        for i in reversed(range(total_players)):
            nickname = player_names[i].split()[0]
            if nickname in player_nicknames:
                name = player_names[i]
                player_nicknames_to_names[nickname] = name
                player_names.remove(name)
                player_nicknames.remove(nickname)

        # If there's only one mismatch, match it up
        if len(player_names) == 1 and len(player_nicknames) == 1:
            player_nicknames_to_names[player_nicknames[0]] = player_names[0]
            player_names.remove(player_names[0])
            player_nicknames.remove(player_nicknames[0])

        # Do hacky matching to figure out remaining name/nicknames string trying to match from beginning of string
        for i in reversed(range(len(player_names))):
            name = player_names[i]
            for i in range(len(name)):
                matching_nickname = ""
                matches_found = 0
                for nickname in player_nicknames:
                    if name[:i+1] == nickname[:i+1]:
                        matching_nickname = nickname
                        matches_found += 1
                if matches_found == 1:
                    player_nicknames_to_names[matching_nickname] = name
                    player_names.remove(name)
                    player_nicknames.remove(matching_nickname)
                    break
        
        if len(player_names) > 0:
            print("could not match all names to nicknames")
            return
    
        player_scores = []
    
        for i in range(total_players):
            player_scores.append([
                scores_table_cb.find_all("td", class_="score_player_nickname")[i].get_text(),
                int(scores_table_cb.find_all("td", class_=regex.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")),
                int(scores_table_j.find_all("td", class_=regex.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")) if scores_table_j else None,
                int(scores_table_dj.find_all("td", class_=regex.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")) if scores_table_dj else None,
                int(scores_table_fs.find_all("td", class_=regex.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")) if scores_table_fs else None,
                int(scores_table_cs.find_all("td", class_=regex.compile("score_(positive|negative)"))[i].get_text().replace("$", "").replace(",", "")) if scores_table_cs else None
            ])

        player_final_scores = []
        
        for i in range(total_players):
            player_final_scores.append(player_scores[i][4])

        player_ranks = len(player_final_scores) - rankdata(player_final_scores, method="min").astype(int) + 1
        
        for i in range(total_players):
        #for name, nickname in player_names_to_nicknames.items():
            nickname = player_scores[i][0]
            name = player_nicknames_to_names[nickname]
            scores = player_scores[i]
            p_id = player_ids[name]
            sql.execute("UPDATE players SET nickname = ? WHERE player_id = ?;", (nickname, p_id,))
            sql.execute("UPDATE game_players SET place = ?, first_break_score = ?, first_round_score = ?, second_round_score = ?, final_score = ?, coryat_score = ? WHERE game_id = ? AND player_id = ?;", (player_ranks.item(i), scores[1], scores[2], scores[3], scores[4], scores[5], gid, p_id,))

        return len(contestants) >= 3 and scores_table_cb and scores_table_j and scores_table_dj and scores_table_fs and scores_table_cs

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
            value = a.find("td", class_=regex.compile("clue_value")).get_text().lstrip("D: $").replace(",", "")
            is_dd = 1 if not not a.find("td", class_="clue_value_daily_double") else 0
            order_number_td = a.find("td", class_=regex.compile("clue_order_number"))
            order_number = order_number_td.find("a").get_text() if order_number_td.find("a") else order_number_td.get_text()
            text = a.find("td", class_="clue_text").get_text()
            answer = BeautifulSoup(a.find("div", onmouseover=True).get("onmouseover"), "lxml")
            right_player_td = answer.find("td", class_="right")
            wrong_player_tds = answer.find_all("td", class_="wrong")
            wrong_answers = []
            
            #print(text)
            
            for wrong_player_td in wrong_player_tds:
                wrong_player_nickname = wrong_player_td.get_text().replace("\\'", "'")
                if wrong_player_nickname == "Triple Stumper" or wrong_player_nickname == "Quadruple Stumper":
                    wrong_player_tds.remove(wrong_player_td)

            answer_table = regex.findall(".*'(\(.*?)(?=\<em)", str(answer).replace("\\'", "'"))
            answer_table_text = str(answer_table[0]) if len(answer_table) > 0 else ""
            wrong_answer_text_matches = regex.finditer("\((([^()]|(?R))*)\)", answer_table_text)
            
            wrong_answer_texts = []
            
            for match in wrong_answer_text_matches:
                split = regex.findall("(.*?) ?[:;,-]+ ?(.*)", match.group(1))
                if len(split) > 0 and len(split[0]) > 1:
                    wrong_answer_texts.append([split[0][0], split[0][1]])
                else:
                    wrong_answer_texts.append(["(no name)", match.group(1)])

            if len(wrong_player_tds) == 1:
                wrong_player_nickname = wrong_player_tds[0].get_text().replace("\\'", "'")
                wrong_answer_text = wrong_answer_texts[0][1] if len(wrong_answer_texts) > 0 else None
                wrong_answers.append([wrong_player_nickname, wrong_answer_text, ])
            elif len(wrong_player_tds) > 1:
                # Another hacky char-by-char match on wrong answers due to dirty data
                for match in wrong_answer_texts:
                    name = match[0]
                    if name == "Alex":
                        found_match = False
                        for td in wrong_player_tds:
                            if name == td.get_text():
                                found_match = True
                        if not found_match:
                            continue
                    for i in range(len(name)):
                        matches_found = 0
                        for td in wrong_player_tds:
                            nickname = td.get_text().replace("\\'", "'")
                            if name[:i+1] == nickname[:i+1]:
                                wrong_player_nickname = nickname
                                wrong_answer_text = match[1]
                                matches_found += 1
                        if matches_found == 1:
                            break
                    if matches_found == 1:
                        wrong_answers.append([wrong_player_nickname, wrong_answer_text, ])
                
            right_player = right_player_td.get_text() if right_player_td else None
            answer = answer.find("em", class_="correct_response").get_text()
            clue_id = insert(sql, [gid, airdate, rnd, categories[x], value, text, answer, game_number, right_player, order_number, is_dd])
            
            for wrong_answer in wrong_answers:
                #print(str(gid) + ', ' + wrong_answer[0])
                p_id = sql.execute("SELECT players.player_id FROM players JOIN game_players ON players.player_id = game_players.player_id AND game_players.game_id = ? WHERE (players.nickname = ? OR players.name = ?)", (gid, wrong_answer[0], wrong_answer[0], )).fetchone()[0]
                #print("INSERT INTO clue_wrong_answers VALUES(" + str(clue_id) + ", " + str(p_id) + ", " + str(wrong_answer[1]) + ")")
                sql.execute("INSERT OR IGNORE INTO clue_wrong_answers VALUES(?, ?, ?)", (clue_id, p_id, wrong_answer[1], ))

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
    sql.execute("INSERT OR IGNORE INTO categories(category) VALUES(?);", (clue[3], ))
    category_id = sql.execute("SELECT category_id FROM categories WHERE category=?;", (clue[3], )).fetchone()[0]
    
    right_player_id = sql.execute("SELECT players.player_id FROM players JOIN game_players ON game_players.player_id = players.player_id WHERE game_players.game_id=? AND players.nickname=?", (clue[0], clue[8].replace("\\'", "'"))).fetchone()[0] if clue[8] else None
    clue_id = sql.execute("INSERT INTO clues(game_id, round, value, category_id, clue, answer, answer_player_id, order_number, is_daily_double) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);", (clue[0], clue[2], clue[4], category_id, clue[5], clue[6], right_player_id, clue[9], clue[10], )).lastrowid
    
    return clue_id

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
