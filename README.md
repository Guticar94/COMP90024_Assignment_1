# COMP90024 Assignment 1

<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->
[![All Contributors](https://img.shields.io/badge/all_contributors-2-orange.svg?style=flat-square)](#contributors-)

## Table of Contents

  1. [Repository Structure](#repository-structure)
  1. [How to run](#repository-structure)
  2. [Contributors](#contributors)
  3. [License](#license)


## Repository Structure
Here is the skeleton of the current repository
```bash
├── input/      # Folder for twitter dataset
│   ├── sal.json       # sal file
├── output/            # Detailed Results of experiments
│   ├── results-1-node-1-core/       # results task 1
│   ├── results-1-node-8-core/       # results task 2
│   ├── results-2-node-8-core/       # results task 3
├── utils/     # Folder for helper functions and variables
├── .gitignore
├── main.py      # Entry point of parallelized application
├── LICENSE
├── README.md
├── requirements.txt    # Require python packages
├── sbatch-1node-1core.sh   # Bash job of Task 1  
├── sbatch-1node-8core.sh   # Bash job of Task 2  
├── sbatch-2node-8core.sh   # Bash job of Task 3  
```

## How to run

**Step 1:** Change directoy to repository root folder
```bash
@spartan-login3 ~]$ cd COMP90024_ASSIGNMENT_1/
```
**Step 2:** Run sbatch over a selected expirement

***Scenario 1:***

```bash
COMP90024_ASSIGNMENT_1]$ sbatch sbatch-1node-1core.sh
```
***Scenario 2:***

```bash
COMP90024_ASSIGNMENT_1]$ sbatch sbatch-1node-8core.sh
```
***Scenario 3:***

```bash
COMP90024_ASSIGNMENT_1]$ sbatch sbatch-2node-8core.sh
```

## Contributors ✨

Thanks to this great team!

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://www.linkedin.com/in/andres-felipe-gutierrez-carre%C3%B1o-62242378/"><img src="https://drive.google.com/uc?export=view&id=1IDlBkB-iwdk8pSx7dLAciDCAumrnsGhq" width="100px;" alt=""/><br /><sub><b>Andrés Gutierrez</b></sub></a><br /></td>
    <td align="center"><a href="https://www.linkedin.com/in/hernan-romano/"><img src="https://drive.google.com/uc?export=view&id=1HvSWAosvqdcaSVBOT7gVtWreXuNPY72Q" width="100px;" alt=""/><br /><sub><b>Hernán Romano</b></sub></a><br /></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

## License
This project is distributed under [GNU GENERAL PUBLIC LICENSE](https://www.gnu.org/licenses/gpl-3.0.en.html)
