defmodule RaftedValue.Transfer do
	use GenServer, restart: :temporary

	def start_link(path) do
	  GenServer.start_link(__MODULE__, path)
	end

	def init(path) do
		GenServer.cast(self(), :init)
		{:ok, path}
	end

	def stream(pid, chunk_size) do
		Stream.unfold(pid, fn(pid)-> 
			case __MODULE__.more(pid, chunk_size) do
				:eof -> nil
				data -> {data, pid}
			end
		end)
	end
	def more(pid, bytes) do
		GenServer.call(pid, {:more, bytes})
	end
	def finish(pid) do
		GenServer.cast(pid, :finish)
	end
	def handle_call({:more, bytes}, _from, file) do
		data = case IO.binread(file, bytes) do
			:eof -> 
				__MODULE__.finish(self())
				:eof
			data -> data
		end
		{:reply, data, file}
	end
	def handle_cast(:finish, file) do
		File.close(file)
		{:stop, :normal, :client_finish}
	end
	def handle_cast(:init, path) do
		{:noreply, File.open!(path, [:read])}
	end
end
