import os, re, sys, json, argparse
import matplotlib.pyplot as plt
import networkx as nx
from pathlib import Path

# output from:  dbt ls --output json > models.json

# TODO add argparser for file location, name of target model and whether or not to include STG



def process_args():
    '''
    Expects a file to have been generated from:   dbt ls --output json > models.json
    The default path is to the projects/repo-dbt-snowflake

    TODO determine if the filename contains a /, and if so, use that as the path.
    '''

    dbt_path = f"C:/Users/{os.environ['USERNAME'].replace('_x','')}/Desktop/repo-dbt-engineering/dbt_root/projects/repo-dbt-snowflake/"

    try:
        parser = argparse.ArgumentParser(description='command line args',epilog="Example:python dbt_models_json_parser.py --output models.json --model DIM_CUSTOMER", add_help=True)
        #required
        parser.add_argument( '--model',help='Target Model to include in the graph')

        #optional
        parser.add_argument( '--output', default='models.json', help='Name of the json file to read in.  Default is models.json')
        parser.add_argument( '--stage', default=False, action='store_true',help='Determine if staging tables should be included in the graph')

        args = parser.parse_args()
    except:
        print(inf.geterr())
        print(f'Example: python {sys.argv[0]} --output models.json --model DIM_CUSTOMER')
        sys.exit(2)

    args.dbt_path = dbt_path

    return args


def read_file( args ):
    filename = args.output
    
    # If a complete path was given, use that, otherwise assume it is in the repo-dbt-snowflake folder
    if '/' in filename:
        file2read = filename
    else:
        dbt_path = args.dbt_path
        file2read  = Path( dbt_path + filename )

    print( f'Attempting to read: {file2read}... ', end='' )
    dependencies = []

    with open( file2read ) as f:
        for jsonObj in f:
            modelDict = json.loads( jsonObj )
            dependencies.append( modelDict )

    # Remove non-models from dictionary
    for idx, item in enumerate( dependencies):
       if not item['resource_type'] == 'model':
        del dependencies[idx]
    
    print( 'Done.' )
    return dependencies



def check_deps( starting_list, dependencies ):
    '''
        For each model, add the dependencies (depends_on: nodes: ) to the starting_list
        Increment the counter and finish the loop.  If any model was added to the list, check to see if it had upstream dependencies too.
    '''

    added = 0

    for item in dependencies:
        if item.get('resource_type','UNK') == "model":      # If it's a model, not a test, staging, etc.
            model = item.get('alias','')
            
            if '.' in model:
                model = model.rsplit('.',1)[1]

            # Only go forward if the model is not a STG table.
            if model.startswith('STG_'):
                pass
            elif model in starting_list : # or each in starting_list:
                needs = item.get('depends_on','')
                needed_nodes = needs.get('nodes','')
                for idx, each in enumerate( needed_nodes ):
                    # check to see if it's a model, to prevent STAGING from appearing in the list of dependencies
                    # should check for args.stage to determine if we should check for startswith
                    #
                    if '.' in each:
                        each = each.rsplit('.',1)[1]
                        # each = re.sub(r"(\w+).(\w+).(\w+)", r"\3", each).strip()
                    
                        if each not in starting_list:
                            starting_list.append( each )
                            added += 1

    return added, starting_list




def drawing( tgt, test_models, starting_list, args ):
    # G = nx.Graph()
    G = nx.MultiDiGraph()

    # tgt =  args.model

    
    # Get starting unique_id:
    for model in test_models:
        if model['resource_type'] == 'model':
            this_id = model['alias']

            if this_id in starting_list:
                needs = model.get('depends_on')
                if needs:
                    needed_nodes = needs.get('nodes','')
                    for _idx, next_iter in enumerate( needed_nodes ):
                        if not (next_iter.startswith('source.') or next_iter.startswith('snapshot.')):
                            # G.add_edge(this_id, next_iter, weight=1)
                            if '.' in this_id:
                                this_id = this_id.rsplit('.',1)[1]
                            if '.' in next_iter:
                                next_iter = next_iter.rsplit('.',1)[1]

                            if next_iter in starting_list:
                                G.add_edge( next_iter, this_id, type='src' )
        
    # print( f'\n====================\nNODES: {G.nodes()}')
    # print( f'\n\n====================\nEDGES: {G.edges()}')

    # Make the Target node red and 10x larger than other nodes
    node_sizes = [10 if node != tgt else 1000 for node in G.nodes ]
    node_color = ['b' if node != tgt else 'r' for node in G.nodes ]


    pos = nx.spring_layout(G, seed=20)

    nx.draw_networkx_nodes(G, pos)
    nx.draw_networkx_labels(G, pos, font_size=6)
    nx.DiGraph(G)

    nx.draw(G, pos, with_labels=False, connectionstyle='arc3, rad = 0.1', node_size = node_sizes, node_color = node_color)
    # edge_labels=dict([((u,v,),d['length']) for u,v,d in G.edges( data=True )])

    plt.show()


def main():
    starting_list = []
    to_process = {}
    
    args = process_args()

    if args.model:
        wanted_item = args.model
    else:
        wanted_item = 'DIM_ADMISSION'

    print( 'Starting... ')
    # Read in the file, can get the file name from args
    dependencies = read_file( args )

    starting_list.append(wanted_item)

    print(f'Starting List, targetting: {starting_list}')

    added, starting_list = check_deps( starting_list, dependencies )

    while added > 0:
        added, starting_list = check_deps( starting_list, dependencies )

    unique_list = set( starting_list )

    drawing( wanted_item, dependencies, starting_list, args )

    print( f'\n\nPLOTTED: {wanted_item}: {unique_list}')
    print('Done.')



if __name__ == "__main__":
    main()

'''

After generating the file via:   
dbt ls --output json > models.json
python dbt_models_json_parser.py --list models.json --model DIM_CUSTOMER

'''