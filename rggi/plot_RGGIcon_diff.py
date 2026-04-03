
import pandas as pd
import matplotlib.pyplot as plt

# Plot the difference in emissions allowances with the updated RGGI policies

### Read in files
rggi_new_df = pd.read_csv('rggicon.csv', header=None, names=['year', 'allowance'])
rggi_old_df = pd.read_csv('rggicon_old.csv', header=None, names=['year', 'allowance'])
merged_rggi_df = pd.merge(rggi_new_df, rggi_old_df, on='year', suffixes=('_new', '_old'))



#### Plot yearly absolute values
# Plot the data
plt.figure(figsize=(10, 6))
plt.bar(merged_rggi_df['year'] - 0.2, merged_rggi_df['allowance_new']/1e6, width=0.4, label='New RGGI Allowance', color='blue')
plt.bar(merged_rggi_df['year'] + 0.2, merged_rggi_df['allowance_old']/1e6, width=0.4, label='Old RGGI Allowance', color='red')
#plt.xlabel('Year')
plt.ylabel(r'Millions of Metric tons CO$_2$', fontsize=20)
plt.title('Comparison of RGGI Allowance Update', fontsize=18)
tick_years= merged_rggi_df['year'][::3] # only show x-ticks every 3 years- easier to read
plt.xticks(tick_years, rotation=90, fontsize=16) 
plt.yticks(fontsize=16)
plt.legend(fontsize=18)
plt.tight_layout()
plt.show()



# Plot yearly difference
plt.figure(figsize=(10, 6))
plt.bar(merged_rggi_df['year'][2:], (merged_rggi_df['allowance_new'][2:] - merged_rggi_df['allowance_old'][2:])/1e6)
# Add vertical lines at each year
for year in merged_rggi_df['year']:
    plt.axvline(x=year, color='gray', linestyle='--', alpha=0.6)
#plt.xlabel('Year')
plt.ylabel('Millions of Metric tons CO$_2$', fontsize=20)
plt.title('Difference (new - old) RGGI Allowance Update', fontsize=18)
tick_years= merged_rggi_df['year'][::2] 
plt.xticks(tick_years, rotation=90, fontsize=16) 
plt.yticks(fontsize=16)
plt.legend(fontsize=18)
plt.tight_layout()
plt.show()


# Plot sum across all years of old and sum of new on same bar plot (2 bars total)# Plot yearly difference
plt.figure(figsize=(10, 6))
plt.bar('Old RGGI', merged_rggi_df['allowance_old'][2:].sum()/1e9)
plt.bar('New RGGI', merged_rggi_df['allowance_new'][2:].sum()/1e9)
plt.ylabel('Billions of Metric tons CO$_2$', fontsize=20)
plt.title('Total Allowance from 2012 - 2050', fontsize=18)
plt.xticks(fontsize=18)
plt.yticks(fontsize=16)
plt.legend(fontsize=18)
plt.tight_layout()
plt.show()

# Print the difference of the total allowance change for the update
rggi_sum_new_minus_old = merged_rggi_df['allowance_new'][2:].sum() - merged_rggi_df['allowance_old'][2:].sum()
print(rggi_sum_new_minus_old)