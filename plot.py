import matplotlib.pyplot as plt

# Sample data

# read the thallium_1 file and populate x_values with index and y_values with the values
iterations = []
get_data_bytes = []
client_expose = []
server_expose = []
read_next = []

with open('thallium_1') as f:
    for i, line in enumerate(f):
        tag, time = line.split(":")
        time = time.strip()

        if tag == "get_data_bytes":
            get_data_bytes.append(int(time))
        elif tag == "clientexpose":
            client_expose.append(int(time))
        elif tag == "serverexpose":
            server_expose.append(int(time))
        elif tag == "ReadNext":
            read_next.append(int(time))

for i in range(len(get_data_bytes)):
    iterations.append(i)

# Plotting the line
plt.plot(iterations, get_data_bytes, label='total')
plt.plot(iterations, client_expose, label='client expose')
plt.plot(iterations, server_expose, label='server expose')
plt.plot(iterations, read_next, label='read next')


# Adding labels and title
plt.ylabel('microseconds (us)')
plt.xlabel('Iteration')
plt.title('Transport duration breakdown')

# Adding a legend
plt.legend()

# Display the plot
plt.savefig('transport_dur_breakdown.pdf')
