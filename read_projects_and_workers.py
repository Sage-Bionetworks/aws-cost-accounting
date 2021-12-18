#file_path = '/home/jovyan/projects-and-workers.txt'

# returns a dict mapping project to workers last names
def read_projects_and_workers(file_path):
	with open(file_path) as file:
	    lines = file.readlines()
	    project_to_workers = {}
	    looking_for_project = True
	    for line in lines:
	        line = line.strip()
	        if looking_for_project:
	            project = line
	            # TODO parse project
	            looking_for_project = False
	        else:
	            try:
	                int(line[0])
	                looking_for_project=True # end of this project
	            except ValueError:
	                # it must be a name
	                name = line[0:line.find(",")].lower()
	                # clean uplast name, e.g., make lower case
	                names = project_to_workers.get(project, set())
	                if not names:
	                    project_to_workers[project]=names
	                names.add(name)
	    if not looking_for_project:
	        raise Exception("Unexpected end of file")

	return(project_to_workers)
