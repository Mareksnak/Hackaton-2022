from gooddata_sdk import GoodDataSdk, CatalogWorkspace
import datetime

# Provide ASCII art

gd_art = r'''
 __    __    ___  _        __   ___   ___ ___    ___ 
|  |__|  |  /  _]| |      /  ] /   \ |   |   |  /  _]
|  |  |  | /  [_ | |     /  / |     || _   _ | /  [_ 
|  |  |  ||    _]| |___ /  /  |  O  ||  \_/  ||    _]
|  `  '  ||   [_ |     /   \_ |     ||   |   ||   [_ 
 \      / |     ||     \     ||     ||   |   ||     |
  \_/\_/  |_____||_____|\____| \___/ |___|___||_____|

                     ______   ___                    
                    |      | /   \                   
                    |      ||     |                  
                    |_|  |_||  O  |                  
                      |  |  |     |                  
                      |  |  |     |                  
                      |__|   \___/                   

     ____   ____  ____   ______  __ __    ___  ____  
    |    \ /    ||    \ |      ||  |  |  /  _]|    \ 
    |  o  )  o  ||  _  ||      ||  |  | /  [_ |  D  )
    |   _/|     ||  |  ||_|  |_||  _  ||    _]|    / 
    |  |  |  _  ||  |  |  |  |  |  |  ||   [_ |    \ 
    |  |  |  |  ||  |  |  |  |  |  |  ||     ||  .  \
    |__|  |__|__||__|__|  |__|  |__|__||_____||__|\_|
'''

panther_art = r'''
    :~-._                                                 _.-~:
    : :.~^o._        ________---------________        _.o^~.:.:
     : ::.`?88booo~~~.::::::::...::::::::::::..~~oood88P'.::.:
     :  ::: `?88P .:::....         ........:::::. ?88P' :::. :
      :  :::. `? .::.            . ...........:::. P' .:::. :
       :  :::   ... ..  ...       .. .::::......::.   :::. :
       `  :' .... ..  .:::::.     . ..:::::::....:::.  `: .'
        :..    ____:::::::::.  . . ....:::::::::____  ... :
       :... `:~    ^~-:::::..  .........:::::-~^    ~::.::::
       `.::. `\   (8)  \b:::..::.:.:::::::d/  (8)   /'.::::'
        ::::.  ~-._v    |b.::::::::::::::d|    v_.-~..:::::
        `.:::::... ~~^?888b..:::::::::::d888P^~...::::::::'
         `.::::::::::....~~~ .:::::::::~~~:::::::::::::::'
          `..:::::::::::   .   ....::::    ::::::::::::,'
            `. .:::::::    .      .::::.    ::::::::'.'
              `._ .:::    .        :::::.    :::::_.'
                 `-. :    .        :::::      :,-'
                    :.   :___     .:::___   .::
          ..--~~~~--:+::. ~~^?b..:::dP^~~.::++:--~~~~--..
            ___....--`+:::.    `~8~'    .:::+'--....___
          ~~   __..---`_=:: ___gd8bg___ :==_'---..__   ~~
           -~~~  _.--~~`-.~~~~~~~~~~~~~~~,-' ~~--._ ~~~-
              -~~            ~~~~~~~~~            ~~-
'''

# GoodData Cloud inputs
host = "https://services.cloud.gooddata.com"
token = ""
sdk = GoodDataSdk.create(host, token)
print(gd_art)
print(panther_art)

print(f"[{datetime.datetime.now()}]  Connected to {host}")

# Initiate variables/script parameters
service_ws_id = 'hack_service'
root_ws_id = 'hack_root'
insight_id = '8e1f0762-cc98-4350-a45f-961061301bfb'  # sf
# insight_id = 'da10e56f-d253-4569-a72f-8b17fdb63eb2'  # pg
safe_delete = False  # WS Delete
parent_safe_delete = False  # Parents delete


# Provide function definitions
def panther_data_from_insight(service_ws_id, insight_id):
    insight = sdk.insights.get_insight(service_ws_id, insight_id)
    table = sdk.tables.for_insight(service_ws_id, insight)
    new_list = []
    for row in table.read_all():
        new_list.append(
            CatalogWorkspace(workspace_id=row[table.attributes[0].local_id], name=row[table.attributes[1].local_id],
                             parent_id=row[table.attributes[2].local_id]))
    return new_list


def get_children(ws_id):
    workspaces = sdk.catalog_workspace.list_workspaces()
    children = []
    for possibleChild in workspaces:
        if possibleChild.parent_id == ws_id:
            # add child that belongs to hierarchy to the output list
            workspaces_in_hierarchy.append(CatalogWorkspace(workspace_id=possibleChild.id, name=possibleChild.name,
                                                            parent_id=possibleChild.parent_id))
            # run search for the child's id as a parent
            children.extend(get_children(possibleChild.id))
    return children


def reset_cache(service_id):
    ws_content = sdk.catalog_workspace_content.get_declarative_ldm(workspace_id=service_id)
    for dataset in ws_content.ldm.datasets:
        sdk.catalog_data_source.register_upload_notification(str(dataset.data_source_table_id.data_source_id))


# Reset service workspace cache

reset_cache(service_ws_id)

# get provisioning data from insight
workspaces_in_hierarchy = []
ws_service = panther_data_from_insight(service_ws_id, insight_id)
# log
print(f"[{datetime.datetime.now()}]  Provisioning data retrieved, total of {len(ws_service)} rows")

get_children(root_ws_id)
# log
print(
    f"[{datetime.datetime.now()}]  All workspaces in {root_ws_id} hierarchy retrieved, total of {len(workspaces_in_hierarchy)} rows")
print()

# main.py
for i in range(len(ws_service)):
    new_ws = True
    for j in range(len(workspaces_in_hierarchy)):
        if ws_service[i].id == workspaces_in_hierarchy[j].id:
            new_ws = False
            if ws_service[i].parent_id == workspaces_in_hierarchy[j].parent_id:
                if ws_service[i].name == workspaces_in_hierarchy[j].name:
                    print(f'[{datetime.datetime.now()}]  Workspace "{ws_service[i].id}" will remain unchanged')
                    # do nothing
                else:
                    print(
                        f'[{datetime.datetime.now()}]  Workspace "{ws_service[i].id}" will change its name from '
                        f'"{ws_service[i].name}" to "{workspaces_in_hierarchy[j].name}"')
                    sdk.catalog_workspace.create_or_update(workspace=ws_service[i])
            else:
                if not parent_safe_delete:
                    try:
                        sdk.catalog_workspace.delete_workspace(workspace_id=ws_service[i].id)
                        sdk.catalog_workspace.create_or_update(workspace=ws_service[i])
                        print(
                            f'[{datetime.datetime.now()}]  Workspace "{ws_service[i].id}" will change its '
                            f'parent workspace from "{ws_service[i].parent_id}" to "'
                            f'{workspaces_in_hierarchy[j].parent_id}"')
                    except Exception as e:
                        print(f'[{datetime.datetime.now()}]  Parent delete error: {e}')
                else:
                    print(
                        f'[{datetime.datetime.now()}]  Workspace "{ws_service[i].id}" will not be modified due to '
                        f'parent safe mode.')

    if new_ws:
        print(f'[{datetime.datetime.now()}]  Creating new workspace with ID "{ws_service[i].id}')
        sdk.catalog_workspace.create_or_update(workspace=ws_service[i])

# check WS to be deleted
for j in range(len(workspaces_in_hierarchy)):
    delete_ws = True
    for i in range(len(ws_service)):
        if ws_service[i].id == workspaces_in_hierarchy[j].id:
            delete_ws = False
    if delete_ws:
        if not safe_delete:
            try:
                print(f'[{datetime.datetime.now()}]  Workspace "{ws_service[i].id}" will be deleted.')
                sdk.catalog_workspace.delete_workspace(workspace_id=workspaces_in_hierarchy[j].id)
            except Exception as e:
                print(f'[{datetime.datetime.now()}]  Workspace "{ws_service[i].id}" delete error: {e}')
        else:
            print(
                f'[{datetime.datetime.now()}]  Workspace "{ws_service[i].id}" will not be deleted due to delete safe '
                f'mode enabled')

# Log
print(f"\n[{datetime.datetime.now()}]  Process ended")