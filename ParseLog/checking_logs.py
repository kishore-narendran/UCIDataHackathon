import pickle


file_names = ['log_data_september_first_half_partial.pickle',
            'log_data_september_second_half_partial.pickle',
            'log_data_october_first_half_partial.pickle',
            'log_data_october_second_half_partial.pickle',
            'log_data_november_first_half_partial.pickle',
            'log_data_november_second_half_partial.pickle']
for file_name in file_names:
    with open(file_name, 'rb') as handle:
        b = pickle.load(handle)
    print b
