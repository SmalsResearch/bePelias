""" Some general configuration for bePelias. """


default_postcode_match_length = 2  # pylint: disable=invalid-name

default_similarity_threshold = 0.8  # pylint: disable=invalid-name

default_transformer_sequence = [
    [],
    ["clean"],
    ["clean", "no_city"],
    ["no_city"],
    ["clean_hn"],
    ["no_city", "clean_hn"],
    ["clean", "no_city", "clean_hn"],
    ["no_hn"],
    ["no_city", "no_hn"],
    ["no_street"],
]

unstruct_transformer_sequence = [  # Transformer sequence used in unstructured_mode
    ["no_city"],
    ["clean", "no_city"],
    ["clean_hn", "no_city"],
    ["clean", "clean_hn", "no_city"],
    [],
    ["clean"],
    ["clean_hn"],
    ["no_hn"],
    ["no_city", "no_hn"],
    ["no_street"],
]

remove_patterns = [(r"\(.+\)$",      ""),
                   ("[, ]*(SN|ZN)$", ""),
                   ("' ", "'"),
                   (" [a-zA-Z][. ]", " "),
                   ("[.]", " "),
                   (",[a-zA-Z .'-]*$", " "),
                   ("[ ]+$", ""),
                   ("^[ ]+", "")
                   ]
