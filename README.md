# huggingface_typo_experiment
## Project Overview
This repository presents an experiment on a method developed to detect impersonation attacks, specifically typosquatting, within model hubs such as Hugging Face. Our study focuses on three key aspects of model hubs: models, datasets, and organizations. Different methods are applied to each aspect, including Levenshtein distance, the SequenceMatcher function from the difflib package, and quantitative analysis.
## Contribution
This project represents the first systematic examination of naming-based vulnerabilities in AI model repositories, uncovering a widespread issue of typosquatting within the Hugging Face ecosystem. The analysis identified models and datasets with potential risks, specifically targeting the top 100 most downloaded models and the top 100 most trending datasets. At the organizational level, our research encompassed all organizations on Hugging Face. We compiled a table listing models, datasets, and organizations exhibiting potentially malicious behavior, emphasizing the need for enhanced governance and security measures within AI model hubs. All suspicious cases have been reported to Hugging Face for further investigation.
## Research objects
   - models
   - datasets
   - organizations
## Repository Structure
   - `dataset/`: Contains the dataset used in this study, with models, datasets, organizations information gathered from Hugging Face.
   - `result/`: Contains models, datasets, and organizations exhibiting potentially malicious behavior.
   - `Similarity caculation/`:
     - similarity_caculation_model: Research on the top 100 most downloaded models
     - similarity_caculation_dataset: Research on the top 100 most trending datasets
     - similarity_caculation_organization: Research on all organiztaions
   - `Similarity analysis/`:
    - similarity_analysis_org: A quantitative analysis on organizations.
   - `README.md\`: Project overview, research context, and usage instructions
## Data Collection
Data for this project was gathered from Hugging Face, including models, datasets, organizations. 
## Key Findings
   - Models: Our analysis reveals 1,574 squatting models targeting top-100 downloads models, with 10.4% exhibiting suspicious behaviors and potential malicious intent through deceptive naming patterns and harmful content manipulation. 
   - Datasets: We discovered 625 cases of typosquatting, where 42.2% demonstrated clear signs of intentional impersonation through misleading metadata and content similarities. 
   - Organizations: We identified 302 instances of squatting behavior, among which 4.8% exhibited explicit malicious intent through active impersonation and deceptive practices, while others showed patterns of preemptive name registration for potential future exploitation.
## Contributing
Contributions to improve detection methods, expand datasets, or provide feedback are welcome. Please submit a pull request or reach out to the repository maintainers.
