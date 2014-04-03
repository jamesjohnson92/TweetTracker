import os
import os.path
import re

entry_filename_regex = r"^\d+\-\d+\.tex$"

jo_files = [ ("Jo",f) for f in os.listdir("Jo") if  re.match(entry_filename_regex,f)]
dilli_files = [ ("Dilli",f) for f in os.listdir("Dilli") if re.match(entry_filename_regex,f)]
howon_files = [ ("Howon",f) for f in os.listdir("Howon") if re.match(entry_filename_regex,f)]



out = open("Journal.tex","w+")
out.write("\\input{Preamble.tex}\n\\begin{document}\n\n\\author{Jo, Dilli and Howon}\\title{CS341 Journal}\\maketitle\n\n")

for p in sorted(jo_files + dilli_files + howon_files, key=lambda a: map(int,re.match(entry_filename_regex,a[1]).groups())):
    out.write("\\section*{%s: %s}\n" % (p[0],p[1][0:-4]))
    out.write("\\input{%s/%s}\n\n"  % p)

out.write("\\end{document}\n")
out.close()


os.system("pdflatex -nonstopmode Journal.tex")

              
