import json
import re

from nested_lookup import nested_lookup
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from seleniumwire import webdriver
from webdriver_manager.chrome import ChromeDriverManager

pga_year = '2020'
pga_tournament = 'shriners-hospitals-for-children-open'

tournament_url = 'https://www.pgatour.com/competition/' + pga_year + '/' + pga_tournament + '/leaderboard.html'

driver = webdriver.Chrome(ChromeDriverManager().install())
driver.get(tournament_url)
# meta_description = driver.find_elements_by_xpath("//meta[@name='description']")[0]
# if meta_description is not None:
#     tournament_course = meta_description.getAttribute("content")

tourn_id_req = driver.wait_for_request(r'https://statdata.pgatour.com/r/\d+', timeout=30)
tourn_id_json = json.loads(tourn_id_req.response.body.decode('utf-8'))
tournament_id = nested_lookup('tid', tourn_id_json)[0]

# alternate tournament id
# torunament_id = driver.find_element_by_xpath("//meta[@name='branch:deeplink:tournament_id']").get_attribute('content')

tourn_detail_req = driver.wait_for_request(
    'https://lbdata.pgatour.com/' + pga_year + '/r/' + tournament_id + '/leaderboard.json', timeout=15)
tourn_detail_json = json.loads(tourn_detail_req.response.body.decode('utf-8'))

cut_line_info = nested_lookup('cutLines', tourn_detail_json)[0]
cut_dict = {}
for i, cut in enumerate(cut_line_info, start=1):
    cut_dict[i] = {
        'cutCount': cut['cut_count'],
        'cutScore': cut['cut_line_score'],
        'cutPaidCount': cut['paid_players_making_cut']
    }

if pga_year != nested_lookup('year', tourn_detail_json)[0]:
    print('Error: Non-matching PGA years. User Input {}; JSON {}'.format(pga_year,
                                                                         nested_lookup('year', tourn_detail_json)[0]))

tournament_info_dict = {
    'tournamentId': tournament_id,
    'cutInfo': cut_dict,
    'format': nested_lookup('format', tourn_detail_json)[0],
    'pgaYear': nested_lookup('year', tourn_detail_json)[0],
    'status': nested_lookup('roundState', tourn_detail_json)[0],
    'playoff': nested_lookup('playoffPresent', tourn_detail_json)[0],
    'dates': driver.find_elements_by_xpath('.//span[@class = "dates"]')[0].text
}

course_ids = set()

player_rows = nested_lookup('rows', tourn_detail_json)[0]
player_name_dict = {}
for row in player_rows:
    player_name_dict[row['playerId']] = {}
    player_name_dict[row['playerId']]['firstName'] = row['playerNames']['firstName']
    player_name_dict[row['playerId']]['lastName'] = row['playerNames']['lastName']
    course_ids.add(row['courseId'])

row_lines = WebDriverWait(driver, 30).until(
    EC.visibility_of_all_elements_located((By.CSS_SELECTOR, 'tr.line-row.line-row')))

player_round_dict = {}
course_meta_dict = {}

for i, row in enumerate(row_lines):
    # if i > 5:
    #     break

    _ = row.location_once_scrolled_into_view
    main_player_id = re.findall(r'\d+', row.get_attribute('class'))[0]
    WebDriverWait(row, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, 'player-name-col'))).click()

    for c_id in course_ids:
        if c_id in course_meta_dict:
            continue

        course_detail_req = driver.wait_for_request(
            'https://lbdata.pgatour.com/' + pga_year + '/r/' + tournament_id + '/course' + c_id, timeout=30)
        course_detail_json = json.loads(course_detail_req.response.body.decode('utf-8'))
        hole_detail_dict = {}
        for hole in nested_lookup('holes', course_detail_json)[0]:
            round_info = {}
            for round_details in hole['rounds']:
                round_detail = {
                    'distance': round_details['distance'],
                    'par': round_details['par'],
                    'stimp': round_details['stimp']
                }
                round_info[round_details['roundId']] = round_detail

            hole_detail_dict[hole['holeId']] = round_info

        course_meta_dict[c_id] = {
            'courseCode': nested_lookup('courseCode', course_detail_json)[0],
            'parIn': nested_lookup('parIn', course_detail_json)[0],
            'parOut': nested_lookup('parOut', course_detail_json)[0],
            'parTotal': nested_lookup('parTotal', course_detail_json)[0],
            'holes': hole_detail_dict
        }

print('SUCCESS')
driver.close()

with open('../tournaments/2020_shriners-hospitals-for-children-open/player_round.json', 'w') as f:
    json.dump(player_round_dict, f)

with open('../tournaments/2020_shriners-hospitals-for-children-open/player_meta.json', 'w') as f:
    json.dump(player_name_dict, f)

with open('../tournaments/2020_shriners-hospitals-for-children-open/tournament_info.json', 'w') as f:
    json.dump(tournament_info_dict, f)

with open('../tournaments/2020_shriners-hospitals-for-children-open/course_meta.json', 'w') as f:
    json.dump(course_meta_dict, f)
