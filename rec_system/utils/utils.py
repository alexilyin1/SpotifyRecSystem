import numpy as np


def list_union(set_list: list)->set:
    start = set(set_list[0])
    for others in set_list[1:]:
        start = start.union(others)
    
    return start


def cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    return np.dot(vec1, vec2) / (np.sqrt(sum([x**2 for x in vec1])) * np.sqrt(sum([x**2 for x in vec2])))


def jaccard_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    return len((set(vec1.tolist()).intersection(set(vec2.tolist())))) / len((set(vec1.tolist()).union(set(vec2.tolist()))))