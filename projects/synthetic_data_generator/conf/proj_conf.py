import os

def get_proj_home():
    """
    Get the project home directory path.
    
    This function determines the project home directory by:
    1. Finding the directory containing the current file
    2. Converting backslashes to forward slashes for cross-platform compatibility
    3. Changing the current working directory to the config directory
    4. Moving up one directory level to reach the project root
    5. Retrieving and normalizing the absolute path
    
    Returns:
        str: The absolute path to the project home directory with forward slashes.
    """
    proj_path = os.path.dirname(os.path.abspath(__file__))
    set_cwdproj_path = proj_path.replace('\\', '/')
    os.chdir(set_cwdproj_path) 
    os.chdir('..')
    proj_home = os.getcwd().replace('\\', '/')
    return proj_home

def get_output_path():
    """
    Retrieve the output directory path for the project.

    Constructs the output directory path by joining the project home directory
    with an 'output' subdirectory. Path separators are normalized to forward slashes.

    Returns:
        str: The absolute path to the project's output directory with forward slashes.
    """
    proj_home = get_proj_home()
    output_path = os.path.join(proj_home, 'output').replace('\\', '/')
    return output_path
