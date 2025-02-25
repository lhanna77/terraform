import os
import subprocess

def has_changes(master_branch, qa_branch):
    
    repo_diff_flag = False
    
    try:
        # Changes required to be applied to the target from the source.
        repo_diff_result = subprocess.check_output(['git', 'log', f'{qa_branch}..{master_branch}', '--oneline']).decode('utf-8')
        
    except subprocess.CalledProcessError as e:
        print(f'Error: {e}')
        return False # Return False in case of an error
        
    if repo_diff_result:
        print(f"\nDifferences - {repo_diff_result}")
        repo_diff_flag = True

    return repo_diff_flag

def merge_branch(master_branch, qa_branch, repo_path='.'):
    try:
        os.chdir(repo_path) # Set the working directory to the Git repository
        
        branches = [master_branch,qa_branch]
        
        for b in branches:
            # Pull latest changes for the branch
            subprocess.run(['git', 'checkout', b], check=True)
            subprocess.run(['git', 'pull'], check=True)
            print(f'Pull successful: {b}')

        # Merge the source branch into the target branch
        if has_changes(master_branch, qa_branch) == True:
            
            subprocess.run(['git', 'merge', master_branch], check=True)
            print(f'\nMerge successful: {master_branch} into {qa_branch}')
            
            subprocess.run(['git', 'push', 'origin', qa_branch], check=True)
            print(f'Push successful: {qa_branch}\n')
            print('\n#####\n')
        else:
            print(f'No differences between {master_branch} and {qa_branch}. \nMerge not needed.\n')
    except subprocess.CalledProcessError as e:
        print(f'Merge failed: {e}')

def main():

    local_repo_path = input("Enter your local repo path e.g. C:\Repo\gbi-looker-prd-dwh : ") 
    local_repo_path = os.path.normpath(local_repo_path)

    print(f"Entered path - {local_repo_path}")

    # MASTER -> BRANCH:
    merge_branch(master_branch='prod', qa_branch='qa', repo_path=local_repo_path)
    merge_branch(master_branch='prod', qa_branch='dev', repo_path=local_repo_path)

if __name__ == "__main__":
    main()
    
# C:\Repo\de-sfmc
# C:\Repo\gbi-sfmc
# C:\Repo\de-pipelines-pubsub
# C:\Repo\gbi-loads-standard
# C:\Repo\gbi-jobs