import regex as re

test_list = ['drinks','total_patients','code50','ID_500c','Stolen Percent','Staked vampires (%)',' front door']
clean_list = []

for i in test_list:
    i = i.replace(' ','_').lower()
    # remove any non-alphanumeric characters
    i = re.sub(r'\W+', '', i)
    # remove any leading/trailing underscores
    i = re.sub(r'^_', '', i)
    i = re.sub(r'_$', '', i)

    clean_list.append(i)

print(clean_list)