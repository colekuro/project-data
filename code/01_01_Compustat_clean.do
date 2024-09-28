// Compustat Fundamentals Quarterly Clean
gl main_path "/Users/colekurokawa/projects/project-data"
gl output_path "$main_path/output"
gl data_path "$main_path/data"
gl raw_path "$main_path/raw"
gl code_path "$main_path/code"

cd "$main_path"

use "$raw_path/compustatpull_09112024.dta", clear

duplicates tag gvkey datadate, gen(tag)

preserve 
drop if tag == 1
tempfile temp 
save `temp'
restore 

drop if tag == 0 

tostring datadate, gen(date)
gen firm_quarter_str = gvkey + "_" + date


levelsof firm_quarter_str, local(levels) 
local first = 1 

foreach fq of local levels { 
	preserve 
	keep if firm_quarter_str == "`fq'" 
	
	foreach var of varlist _all { 
		sort `var' 
		if !mi(`var'[2]) { 
			replace `var' = `var'[2] in 1 
		}
	}
	drop in 2
	
	if `first' == 1 { 
		save "$data_path/temp.dta", replace 
		local first = 0
	}
	else { 
		append using "$data_path/temp.dta"
		save "$data_path/temp.dta", replace 
		local first = 0
	}
	restore
}

// unab vars: _all 
// egen nonmissing = rownonmiss(`vars'), strok


use "$data_path/temp.dta", clear
append using `temp' 


// Create firm-quarter variables for panel
gen qdate = qofd(datadate)
format qdate %tq
order gvkey qdate

drop tag firm_quarter_str date
erase "$data_path/temp.dta"

save "$data_path/compustatpull_09112024_cleaned.dta", replace 

