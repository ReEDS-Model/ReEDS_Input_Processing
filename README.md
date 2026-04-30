# ReEDS Input Processing

This repository is a collection of data and preprocessing scripts that generate input files for [ReEDS](https://github.com/ReEDS-Model/ReEDS), the Regional Energy Deployment System.

This repository is organized into subfolders, each corresponding to a category of inputs for ReEDS. Each subfolder is mostly self-contained and should include its own README.

## Getting Started
**Prerequisites:**
- ReEDS: This repository has a dependency on ReEDS. Make sure you have the [ReEDS](https://github.com/ReEDS-Model/ReEDS) installed.
  - **note:** most scripts are developed assuming the use of the `reeds2` environment from the ReEDS repository
- Python: Most scripts require Python with various packages (see individual subfolder READMEs)

1. **Clone this repository** and the ReEDS repository
2. **Navigate to the subfolder** relevant to the ReEDS input(s) you're working with
3. **Follow the instructions** in that subfolder's README for setup and execution

**NOTE:** If you encounter errors when running a script, you may need to checkout an older version of ReEDS that aligns with when the script was last updated.

## Contributing

### Code and File Guidelines

- **Naming convention**: Use lowercase with underscores for file and folder names
- **Documentation**: Every subfolder should include a comprehensive README (see requirements below)
- **Code style**: Follow the [ReEDS Developer Guide](https://reeds-model.github.io/ReEDS/developer_best_practices.html#coding-standards-and-conventions) standards

### Subfolder README Requirements

Each subfolder README should include:

- **Data description**: Brief description of the data and its original source
- **ReEDS integration**: Which ReEDS input file(s) this processing creates or modifies  
- **Instructions**: Step-by-step guide for running scripts, including:
  - Environment setup requirements
  - Required ReEDS version (if using ReEDS functions)

### File Size Guidelines

Git performs best with small files, but data processing sometimes requires larger files:

- **Direct source pulling**: If raw data can be pulled directly from the original source, don't add it to the repo. Scripts should download it directly
- **Small files (<50MB)**: Can be committed directly to the repository  
- **Large files (≥50MB)**: Upload elsewhere and download during script execution
  - **DVC/LFS**: Preferred for frequently changing files specific to this repository
  - **Zenodo/OpenEI**: Preferred for stable files of broader interest to the research community

## Support

For questions or issues:
1. Check the relevant subfolder README first
2. Review existing [GitHub issues](https://github.com/ReEDS-Model/ReEDS_Input_Processing/issues) and discussions
3. Open a new issue with detailed information about your problem and environment