""" Preload a list of model information that will be parsed from files
"""
test_models = [	{'resource_type': 'model', 'name': 'DIM_CUSTOMER', 'unique_id': 'model.edw.DIM_CUSTOMER', 'depends_on': 'model.edw.DIM_INJURED_WORKER, model.edw.DIM_EMPLOYER, model.edw.DIM_PROVIDER, model.edw.DIM_NETWORK, model.edw.DIM_REPRESENTATIVE', 'check_cols': '', 'original_file_path': 'models/EDW_STAGING_DIM/DIM_CUSTOMER.sql'}, 
	{'resource_type': 'model', 'name': 'DIM_INJURED_WORKER', 'unique_id': 'model.edw.DIM_INJURED_WORKER', 'depends_on': 'model.edw.DIM_INJURED_WORKER_SCDALL_STEP2', 'check_cols': '', 'original_file_path': 'models/EDW_STAGING_DIM/DIM_INJURED_WORKER.sql'}, 
 	{'resource_type': 'model', 'name': 'DIM_INJURED_WORKER_SCDALL_STEP2', 'unique_id': 'model.edw.DIM_INJURED_WORKER_SCDALL_STEP2', 'depends_on': 'model.edw.DSV_INJURED_WORKER, snapshot.edw.DIM_INJURED_WORKER_SNAPSHOT_STEP1', 'check_cols': '', 'original_file_path': 'models/EDW_STAGING/DIM_INJURED_WORKER_SCDALL_STEP2.sql'}, 
	{'resource_type': 'model', 'name': 'DSV_INJURED_WORKER', 'unique_id': 'model.edw.DSV_INJURED_WORKER', 'depends_on': 'model.edw.DST_INJURED_WORKER', 'check_cols': '', 'original_file_path': 'models/STAGING/DSV_INJURED_WORKER.sql'}, 
	{'resource_type': 'model', 'name': 'DST_INJURED_WORKER', 'unique_id': 'model.edw.DST_INJURED_WORKER', 'depends_on': 'model.edw.STG_CUSTOMER_ADDRESS_MAIL, model.edw.STG_CUSTOMER_ADDRESS_PHYS, model.edw.STG_PERSON_HISTORY, model.edw.STG_PERSON, model.edw.STG_PARTICIPATION, model.edw.STG_CUSTOMER_NAME, model.edw.STG_CUSTOMER_SOC_SEC_RET_DTL, model.edw.STG_CUSTOMER_BLOCK, model.edw.STG_CUSTOMER_ADDRESS, model.edw.STG_CUSTOMER_LANGUAGE', 'check_cols': '', 'original_file_path': 'models/STAGING/DST_INJURED_WORKER.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_ADDRESS_MAIL', 'unique_id': 'model.edw.STG_CUSTOMER_ADDRESS_MAIL', 'depends_on': 'model.edw.STG_CUSTOMER_ADDRESS', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_ADDRESS_MAIL.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_ADDRESS_PHYS', 'unique_id': 'model.edw.STG_CUSTOMER_ADDRESS_PHYS', 'depends_on': 'model.edw.STG_CUSTOMER_ADDRESS', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_ADDRESS_PHYS.sql'}, 
	{'resource_type': 'model', 'name': 'STG_PERSON_HISTORY', 'unique_id': 'model.edw.STG_PERSON_HISTORY', 'depends_on': 'source.edw.MEDICAL.PERSON_HISTORY, source.edw.MEDICAL.GENDER_TYPE, source.edw.MEDICAL.MARITAL_STATUS_TYPE, source.edw.MEDICAL.TAX_FILING_STATUS_TYPE, source.edw.MEDICAL.CUSTOMER_HISTORY', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_PERSON_HISTORY.sql'}, 
	{'resource_type': 'model', 'name': 'STG_PERSON', 'unique_id': 'model.edw.STG_PERSON', 'depends_on': 'source.edw.MEDICAL.PERSON, source.edw.MEDICAL.GENDER_TYPE, source.edw.MEDICAL.MARITAL_STATUS_TYPE, source.edw.MEDICAL.TAX_FILING_STATUS_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_PERSON.sql'}, 
	{'resource_type': 'model', 'name': 'STG_PARTICIPATION', 'unique_id': 'model.edw.STG_PARTICIPATION', 'depends_on': 'source.edw.MEDICAL.PARTICIPATION, source.edw.MEDICAL.PARTICIPATION_TYPE, source.edw.MEDICAL.CUSTOMER, source.edw.MEDICAL.CLAIM_PARTICIPATION', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_PARTICIPATION.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_NAME', 'unique_id': 'model.edw.STG_CUSTOMER_NAME', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_NAME, source.edw.MEDICAL.CUSTOMER_NAME_TYPE, source.edw.MEDICAL.CUSTOMER_NAME_TITLE_TYPE, source.edw.MEDICAL.CUSTOMER_NAME_SUFFIX_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_NAME.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_ADDRESS', 'unique_id': 'model.edw.STG_CUSTOMER_ADDRESS', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_ADDRESS, source.edw.MEDICAL.CUSTOMER_ADDRESS_TYPE, source.edw.MEDICAL.STATE, source.edw.MEDICAL.COUNTRY', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_ADDRESS.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_ADDRESS_MAIL', 'unique_id': 'model.edw.STG_CUSTOMER_ADDRESS_MAIL', 'depends_on': 'model.edw.STG_CUSTOMER_ADDRESS', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_ADDRESS_MAIL.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_ADDRESS_PHYS', 'unique_id': 'model.edw.STG_CUSTOMER_ADDRESS_PHYS', 'depends_on': 'model.edw.STG_CUSTOMER_ADDRESS', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_ADDRESS_PHYS.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_BLOCK', 'unique_id': 'model.edw.STG_CUSTOMER_BLOCK', 'depends_on': 'model.edw.STG_CUSTOMER_BLOCK_SOURCE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_BLOCK.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_BLOCK_SOURCE', 'unique_id': 'model.edw.STG_CUSTOMER_BLOCK_SOURCE', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_BLOCK_ROLE_BLOCK, source.edw.MEDICAL.BLOCK, source.edw.MEDICAL.BLOCK_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_BLOCK_SOURCE.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_CHILD_SUPPORT', 'unique_id': 'model.edw.STG_CUSTOMER_CHILD_SUPPORT', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_CHILD_SUPPORT, source.edw.MEDICAL.CUSTOMER, source.edw.MEDICAL.CUSTOMER_NAME, source.edw.MEDICAL.WITHHOLDING_PERIOD_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_CHILD_SUPPORT.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_CONTACT', 'unique_id': 'model.edw.STG_CUSTOMER_CONTACT', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_CONTACT, source.edw.MEDICAL.CUSTOMER, source.edw.MEDICAL.CONTACT_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_CONTACT.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_CONTACT_DETAIL', 'unique_id': 'model.edw.STG_CUSTOMER_CONTACT_DETAIL', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_CONTACT_DETAIL, source.edw.MEDICAL.CUSTOMER_CONTACT_DETAIL_TYPE, source.edw.MEDICAL.PHONE_NUMBER_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_CONTACT_DETAIL.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_INTERACTION_CHANNEL', 'unique_id': 'model.edw.STG_CUSTOMER_INTERACTION_CHANNEL', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_INTERACTION_CHANNEL, source.edw.MEDICAL.CUSTOMER, source.edw.MEDICAL.INTERACTION_CHANNEL_TYPE, source.edw.MEDICAL.CUSTOMER_ROLE_TYPE, source.edw.MEDICAL.PHONE_NUMBER_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_INTERACTION_CHANNEL.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_LANGUAGE', 'unique_id': 'model.edw.STG_CUSTOMER_LANGUAGE', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_LANGUAGE, source.edw.MEDICAL.LANGUAGE_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_LANGUAGE.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_NAME', 'unique_id': 'model.edw.STG_CUSTOMER_NAME', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_NAME, source.edw.MEDICAL.CUSTOMER_NAME_TYPE, source.edw.MEDICAL.CUSTOMER_NAME_TITLE_TYPE, source.edw.MEDICAL.CUSTOMER_NAME_SUFFIX_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_NAME.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_ROLE_ACCOUNT_HOLDER', 'unique_id': 'model.edw.STG_CUSTOMER_ROLE_ACCOUNT_HOLDER', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_ROLE, source.edw.MEDICAL.CUSTOMER_ROLE_TYPE, source.edw.MEDICAL.ROLE_ACCOUNT_HOLDER, source.edw.MEDICAL.SIC_TYPE, source.edw.MEDICAL.ACCOUNT_STATUS_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_ROLE_ACCOUNT_HOLDER.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_ROLE_IDENTIFIER', 'unique_id': 'model.edw.STG_CUSTOMER_ROLE_IDENTIFIER', 'depends_on': 'source.edw.MEDICAL.CUSTOMER_ROLE_IDENTIFIER, source.edw.MEDICAL.IDENTIFIER_TYPE, source.edw.MEDICAL.CUSTOMER_ROLE_TYPE', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_ROLE_IDENTIFIER.sql'}, 
	{'resource_type': 'model', 'name': 'STG_CUSTOMER_SOC_SEC_RET_DTL', 'unique_id': 'model.edw.STG_CUSTOMER_SOC_SEC_RET_DTL', 'depends_on': 'source.edw.MEDICAL.BWC_CUSTOMER_SOC_SEC_RET_DTL', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_CUSTOMER_SOC_SEC_RET_DTL.sql'}, 
	{'resource_type': 'model', 'name': 'DIM_EMPLOYER', 'unique_id': 'model.edw.DIM_EMPLOYER', 'depends_on': 'model.edw.DIM_EMPLOYER_SCDALL_STEP2', 'check_cols': '', 'original_file_path': 'models/EDW_STAGING_DIM/DIM_EMPLOYER.sql'}, 
	{'resource_type': 'model', 'name': 'DIM_PROVIDER', 'unique_id': 'model.edw.DIM_PROVIDER', 'depends_on': 'model.edw.DIM_PROVIDER_SCDALL_STEP2', 'check_cols': '', 'original_file_path': 'models/EDW_STAGING_DIM/DIM_PROVIDER.sql'}, 
 	{'resource_type': 'model', 'name': 'DIM_NETWORK', 'unique_id': 'model.edw.DIM_NETWORK', 'depends_on': 'model.edw.DIM_NETWORK_SCDALL_STEP2', 'check_cols': '', 'original_file_path': 'models/EDW_STAGING_DIM/DIM_NETWORK.sql'}, 
	{'resource_type': 'model', 'name': 'DIM_REPRESENTATIVE', 'unique_id': 'model.edw.DIM_REPRESENTATIVE', 'depends_on': 'model.edw.DIM_REPRESENTATIVE_SCDALL_STEP2', 'check_cols': '', 'original_file_path': 'models/EDW_STAGING_DIM/DIM_REPRESENTATIVE.sql'}, 
	{'resource_type': 'model', 'name': 'DIM_NETWORK_SCDALL_STEP2', 'unique_id': 'model.edw.DIM_NETWORK_SCDALL_STEP2', 'depends_on': 'model.edw.DSV_NETWORK, snapshot.edw.DIM_NETWORK_SNAPSHOT_STEP1', 'check_cols': '', 'original_file_path': 'models/EDW_STAGING/DIM_NETWORK_SCDALL_STEP2.sql'}, 
	{'resource_type': 'model', 'name': 'DSV_NETWORK', 'unique_id': 'model.edw.DSV_NETWORK', 'depends_on': 'model.edw.DST_NETWORK', 'check_cols': '', 'original_file_path': 'models/STAGING/DSV_NETWORK.sql'}, 
	{'resource_type': 'model', 'name': 'DST_NETWORK', 'unique_id': 'model.edw.DST_NETWORK', 'depends_on': 'model.edw.STG_NETWORK', 'check_cols': '', 'original_file_path': 'models/STAGING/DST_NETWORK.sql'}, 
	{'resource_type': 'model', 'name': 'STG_NETWORK', 'unique_id': 'model.edw.STG_NETWORK', 'depends_on': 'source.edw.MEDICAL.CUSTOMER, source.edw.MEDICAL.NTWK, source.edw.MEDICAL.NTWK_STATUS_HISTORY, source.edw.MEDICAL.REF, source.edw.MEDICAL.ADR, source.edw.MEDICAL.ADR_TYP, source.edw.MEDICAL.CUSTOMER_ROLE_IDENTIFIER', 'check_cols': '', 'original_file_path': 'models/STAGING/STG_NETWORK.sql'}, 
]

import matplotlib.pyplot as plt
import networkx as nx


def main():
    # G = nx.Graph()
    G = nx.MultiDiGraph()

    tgt = 'DIM_CUSTOMER'
    
    # Get starting unique_id:
    for model in test_models:
        if model['resource_type'] == 'model':
            this_id = model['unique_id']
            need_list = list( model['depends_on'].split(', ') )
            for _idx, next_iter in enumerate( need_list ):

                if not (next_iter.startswith('source.') or next_iter.startswith('snapshot.')):
                    # G.add_edge(this_id, next_iter, weight=1)
                    this_id = this_id.replace('model.edw.','')
                    next_iter = next_iter.replace('model.edw.','')
                    
                    G.add_edge( next_iter, this_id, type='src' )
    
    print( f'NODES: {G.nodes()}')
    print( f'EDGES: {G.edges()}')


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
    
if __name__ == "__main__":
    main()
    