import networkx as nx
import numpy
import pandas as pd
import os

OUTPUT_DIR="polar_articles"
ANALYSIS_DIR = os.path.join(OUTPUT_DIR, "analysis_results")
os.makedirs(ANALYSIS_DIR, exist_ok=True)

from polarlib.prism.polarization_knowledge_graph import PolarizationKnowledgeGraph

pkg = PolarizationKnowledgeGraph(output_dir = OUTPUT_DIR)
pkg.construct()

# Entity-Level Analysis
from polarlib.prism.multi_level_polarization import EntityLevelPolarizationAnalyzer

entity_level_analyzer = EntityLevelPolarizationAnalyzer()
entity_df = entity_level_analyzer.analyze(pkg, pole_path='./', output_dir=OUTPUT_DIR)
print(entity_df.head(5))
entity_df.to_csv(os.path.join(ANALYSIS_DIR, "entity_level.csv"), index=False, encoding="utf-8")

# Group-Level Analysis
from polarlib.prism.multi_level_polarization import GroupLevelPolarizationAnalyzer

group_analyzer = GroupLevelPolarizationAnalyzer()
attitudinal_cohesiveness = GroupLevelPolarizationAnalyzer.calculate_attitudinal_cohesiveness(pkg)

df1 = []
df2 = []

for i, f in enumerate(pkg.fellowship_list):

    f_label = f'F{i}'

    fg    = pkg.pkg.subgraph(f)
    edges = fg.edges(data=True)

    pos_edges = [(e[0], e[1], {'weight':  1}) for e in list(edges) if e[2]['weight'] > 0.0]
    neg_edges = [(e[0], e[1], {'weight': -1}) for e in list(edges) if e[2]['weight'] < 0.0]

    fgp = nx.Graph()
    fgn = nx.Graph()

    fgp.add_edges_from(pos_edges)
    fgn.add_edges_from(neg_edges)

    topical_cohesiveness = attitudinal_cohesiveness[f_label]

    for t in topical_cohesiveness:

        _ = topical_cohesiveness[t].copy()
        _['fellowship'] = f_label
        _['topic']      = t

        df2.append(_)

    attitudinal_cohesiveness_values = [v['cohesiveness'] for v in topical_cohesiveness.values()]

    df1.append({
        'fellowship':    f_label,
        'edges':         len(edges),
        'positive':      len(pos_edges),
        'negative':      len(neg_edges),
        'density':       nx.density(fg),
        'density_plus':  nx.density(fgp),
        'density_minus': nx.density(fgn),

        'attitudinal_cohesiveness_avg': numpy.mean(attitudinal_cohesiveness_values) if len(attitudinal_cohesiveness_values) > 0 else None,
        'attitudinal_cohesiveness_std': numpy.std(attitudinal_cohesiveness_values)  if len(attitudinal_cohesiveness_values) > 0 else None,
        'attitudinal_cohesiveness_max': max(attitudinal_cohesiveness_values)        if len(attitudinal_cohesiveness_values) > 0 else None,
        'attitudinal_cohesiveness_min': min(attitudinal_cohesiveness_values)        if len(attitudinal_cohesiveness_values) > 0 else None
        })

coh_df = pd.DataFrame.from_dict(df1)
att_df = pd.DataFrame.from_dict(df2)

coh_df.to_csv(os.path.join(ANALYSIS_DIR, "group_cohesion_summary.csv"), index=False, encoding="utf-8")
att_df.to_csv(os.path.join(ANALYSIS_DIR, "group_topic_cohesion.csv"), index=False, encoding="utf-8")

# Topic-Level Analysis
from polarlib.prism.multi_level_polarization import TopicLevelPolarizationAnalyzer

topic_analyzer = TopicLevelPolarizationAnalyzer()
local_df, global_df = topic_analyzer.analyze(pkg)

print(local_df.head(10))
print(global_df.head(10))

local_df.to_csv(os.path.join(ANALYSIS_DIR, "analyzer_results.csv"), index=False, encoding="utf-8")
global_df.to_csv(os.path.join(ANALYSIS_DIR, "global_polarization.csv"), index=False, encoding="utf-8")