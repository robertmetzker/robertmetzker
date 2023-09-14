test = 'Release Period,State,Facility ID,Completed Surveys,Response Rate (%)'
# Split test into a list
# Remove any non-alphanumeric characters from each list item
# Join the list items back into a string

# Split test into a list
test_list = test.split(',')
print(test_list)

# Remove any non-alphanumeric characters from each string in the list
for i in range(len(test_list)):
    test_list_str = test_list[i]
    test_list_str = test_list_str.replace(' ', '_')
    test_list[i] = ''.join(e for e in test_list_str if e.isalnum())

print(test_list)

# Join the list items back into a string
test = ','.join(test_list)

print(test)

