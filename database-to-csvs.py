import sqlite3
import pandas.io.sql as sql

con = sqlite3.connect('output/database.db')

def export(con, table_name):
    table = sql.read_frame('select * from ' + table_name, con)
    filename = 'output/' + table_name + '.csv'
    print('starting: ' + filename)
    table.to_csv(filename, index=False)

export(con, 'categories')
export(con, 'clue_wrong_answers')
export(con, 'clues')
export(con, 'final_jeopardy_answers')
export(con, 'game_players')
export(con, 'games')
export(con, 'players')
