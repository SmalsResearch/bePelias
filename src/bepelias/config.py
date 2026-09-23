""" Some general configuration for bePelias. """


default_postcode_match_length = 2  # pylint: disable=invalid-name

default_similarity_threshold = 0.8  # pylint: disable=invalid-name

default_transformer_sequence = [
    [["struct"], ["no_city"]],
    [["struct"], ["clean", "no_city"]],
    [["struct"], []],

    [["unstruct"], []],
    [["unstruct"], ["clean"]],
    [["unstruct"], ["clean", "no_city"]],

    [["struct"], ["clean"]],

    [["struct", "unstruct"], ["clean_hn"]],
    [["struct", "unstruct"], ["no_city", "clean_hn"]],
    [["struct", "unstruct"], ["clean", "no_city", "clean_hn"]],
    [["struct", "unstruct"], ["no_hn"]],
    [["struct", "unstruct"], ["no_city", "no_hn"]],
    [["struct", "unstruct"], ["no_street"]]
]

unstruct_transformer_sequence = [  # Transformer sequence used in unstructured_mode
    [["struct", "unstruct"], ["no_city"]],
    [["struct", "unstruct"], ["clean", "no_city"]],
    [["struct", "unstruct"], ["clean_hn", "no_city"]],
    [["struct", "unstruct"], ["clean", "clean_hn", "no_city"]],
    [["struct", "unstruct"], []],
    [["struct", "unstruct"], ["clean"]],
    [["struct", "unstruct"], ["clean_hn"]],
    [["struct", "unstruct"], ["no_hn"]],
    [["struct", "unstruct"], ["no_city", "no_hn"]],
    [["struct", "unstruct"], ["no_street"]]
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
