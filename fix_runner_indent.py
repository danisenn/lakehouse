with open("backend/src/assistant/runner.py", "r") as f:
    lines = f.readlines()

for i in range(75, 203):
    if lines[i].strip():
        lines[i] = "    " + lines[i]

with open("backend/src/assistant/runner.py", "w") as f:
    f.writelines(lines)
print("Indentation fixed.")
