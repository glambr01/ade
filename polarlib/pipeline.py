import spacy
from polarlib.polar.attitude.syntactical_sentiment_attitude import SyntacticalSentimentAttitudePipeline
from polarlib.polar.news_corpus_collector import *
from polarlib.polar.actor_extractor import *
from polarlib.polar.topic_identifier import *
from polarlib.polar.coalitions_and_conflicts import *
from polarlib.polar.sag_generator import *

OUTPUT_DIR = "polar_articles"

if __name__ == "__main__":

    # Extract entities
    entity_extractor = EntityExtractor(output_dir=OUTPUT_DIR, coref=False)
    entity_extractor.extract_entities()

    # Extract noun phrases
    noun_phrase_extractor = NounPhraseExtractor(output_dir=OUTPUT_DIR)
    noun_phrase_extractor.extract_noun_phrases()

    # Identify topics
    topic_identifier = TopicIdentifier(output_dir=OUTPUT_DIR, llama_wv=False)
    topic_identifier.encode_noun_phrases()
    topic_identifier.noun_phrase_clustering(threshold=0.8)

    # Sentiment Attitude
    sentiment_attitude_pipeline = SyntacticalSentimentAttitudePipeline(
        output_dir = OUTPUT_DIR,
        nlp = spacy.load("en_core_web_sm"),
        mpqa_path = "subjclueslen1-HLTEMNLP05.tff"
    )

    sentiment_attitude_pipeline.calculate_sentiment_attitudes()

    # Build SAG
    sag_generator = SAGGenerator(OUTPUT_DIR)

    sag_generator.load_sentiment_attitudes()

    bins = sag_generator.calculate_attitude_buckets(
        verbose=True,
        figsize=(16,4)
    )

    sag_generator.convert_attitude_signs(
        bin_category_mapping = {
            "NEGATIVE":[(-1.00,-0.02)],
            "NEUTRAL":[(-0.02,0.02)],
            "POSITIVE":[(0.02,1.00)]
        },
        minimum_frequency = 5,
        verbose = True
    )

    G, node_to_int, int_to_node = sag_generator.construct_sag()

    # Fellowships
    fellowship_extractor = FellowshipExtractor(OUTPUT_DIR)

    fellowships = fellowship_extractor.extract_fellowships(
        n_iter = 10,
        resolution = 0.075,
        merge_iter = 10,
        jar_path ='./polarlib',
        verbose = True,
        output_flag = True
    )

    # Dipoles
    dipole_generator = DipoleGenerator(OUTPUT_DIR)
    dipoles = dipole_generator.generate_dipoles(
        f_g_thr=0.7,
        n_r_thr=0.5
    )

    # Topic polarization
    topic_attitude_calculator = TopicAttitudeCalculator(OUTPUT_DIR)

    topic_attitude_calculator.load_sentiment_attitudes()

    dipole_topics_dict = topic_attitude_calculator.get_polarization_topics()

    topic_attitudes = topic_attitude_calculator.get_topic_attitudes()

    print("POLAR pipeline completed.")