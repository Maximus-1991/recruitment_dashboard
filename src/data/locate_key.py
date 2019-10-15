"""
Function to retrieve location of a key in nested data structure
"""

def locate_element(data, look_up_elem):
    '''
    Function to locate the exact location of a an element in a data structure
    '''
    data_orig = data
    loc_list = []

    #### Step 1: Create loop: while look_up_elem not in loc_list
    while look_up_elem not in loc_list:

        data = data_orig
        if loc_list != []:
            for location in loc_list:
                data = data[location]

                #### Step 2: Create loop for each element in data.
                # This element needs to be appended to loc_list if element is
                # found in (sub-levels of) this element

        # Combine step 4 and 5 in one function.
        # Function is to flatten the data and check if look_up_elem is present in data.
        # If element is found, return loc_list
        def check_branche(data):
            #### Step 2: Check if look_up_element is present on 1st level of data
            if look_up_elem in data:
                loc_list.append(look_up_elem)
                return loc_list

            #### Step 3: If element not present on 1st level, filter out strings and integers from data.
            # Method is different for different data types
            # Note: data_tuple = () (will be problematic, as you cannot append elements to a tuple).
            # We may be able to add items from tuple to list as tuple is also a Sequence

            # Define data_elements
            if isinstance(data, dict):
                data_elements = list(data.keys())
            elif isinstance(data, list):
                data_elements = list(range(len(data)))
            # elif type(data)==tuple:
            # data_elements = list(range(len(data)))

            else:
                return "Element not present"

            for element in data_elements:
                data_to_check = data[element]

                # Define data_dict, data_list and data_tuple
                if isinstance(data_to_check, dict):
                    data_dict = data_to_check
                    data_list = []
                    data_tuple = ()
                elif isinstance(data_to_check, list):
                    data_dict = {}
                    data_list = data_to_check
                    data_tuple = ()
                elif isinstance(data_to_check, tuple):
                    data_dict = {}
                    data_list = []
                    data_tuple = data_to_check
                elif not isinstance(data_to_check, dict) and not isinstance(data_to_check, list) \
                and not isinstance(data_to_check, tuple):
                    continue
                else:
                    return "Error"

                #### Step 5: Enter while loop (is within the for loop of step 4).
                # From the filtered data obtained in step 3, divide the different elements into its data type
                # Then, flatten type(data) data type first and then the other two data types
                # When look_up_elem is found, append element to loc_list and return loc_list
                while data_dict != {} or data_list != [] or data_tuple != ():
                    # Flatten dictionary and check if element is present on any of the levels and
                    # add list elements to data_list
                    # After first round, if any elements were added to data_dict,
                    # go through these added elements
                    while data_dict != {}:
                        if look_up_elem in data_dict:
                            loc_list.append(element)
                            return

                        data_dict_temp = {}
                        # Filter the elements in data_dict
                        for key, value in iter(data_dict.items()):
                            if isinstance(value, dict):
                                data_dict_temp.update(value)
                            elif isinstance(value, list):
                                data_list.append(value)
                            # elif isinstance(value, tuple):
                            #    test_tuple
                            # to check if tuple is also a sequence, can also use chain(element) for this
                            else:
                                continue
                        data_dict = data_dict_temp

                    # After data_dict is (temporarily) exhausted, go through data_list
                    while data_list != []:
                        if look_up_elem in data_list:
                            loc_list.append(element)
                            return loc_list

                        data_list_temp = []
                        # Filter the elements in data_dict
                        for item in data_list:
                            if isinstance(item, dict):
                                data_dict.update(item)
                            elif isinstance(item, list):
                                for idx in item:
                                    data_list_temp.append(idx)
                            else:
                                continue
                        data_list = data_list_temp

                    # After data_list is (temporarily) exhausted, go through data_tuple
                    # while data_tuple !=():
                    # Flatten tuple, check if element is present on any of the levels and add dictionary
                    # and list elements to data_dict or data_list
                    # pass

            if look_up_elem not in loc_list:
                return "Element not Found"

        check_branche(data)
    return loc_list
