require 'indexes'

OP_READ_INDEX = 6
OP_WRITE_INDEX = 7
OP_ERASE_INDEX = 8
NUM_PAGES_WRITTEN_INDEX = 5

def readIn(fileName, execName = nil)
	execName = fileName if (!execName)
	puts "Reading the filename #{fileName}, #{execName}"
	values = []
	File.readlines(fileName).each{|x| 
		x.chomp!
        x.strip!
		next if (x.empty? || x=~/^#/)
		arr = x.split.map{|y| y.to_f}
		next if (arr.empty?)
		values << arr
	}
	vals = {}

	vals["nValues"] = values.first.slice!(1)
	vals["nQueries"] = values.first.slice!(1)
	vals["nKeysInserted"] = values.first.slice!(1)
	vals["nodeSize"] = values.first.slice!(1)
	vals["bufferSize"] = values.first.slice!(1)
	vals["numNodes"] = values.first.slice!(1)
	vals["height"] = values.first.slice!(1)
	vals["realNodeSize"] = values.first.slice!(1)
	vals["numBuffers"] = values.first.slice!(1)
    vals["nKeysInsertedActually"] = values.first.slice!(1)

    #Done with the meta line ... remove it. 
    values.slice!(1)

	vals["indexReadCost"] = values.first[OP_READ_INDEX]
	vals["indexWriteCost"] = values.first[OP_WRITE_INDEX]
	vals["indexErase"] = values.first[OP_ERASE_INDEX]
	vals["indexPagesWritten"] = values.first[NUM_PAGES_WRITTEN_INDEX]

	puts "Returning #{vals.inspect}"
	return vals
end
