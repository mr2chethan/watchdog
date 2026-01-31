from backend.cfo_agent import CFOAgent
import sys

def test():
    print("Testing CFO Agent...")
    try:
        agent = CFOAgent()
        agent._ensure_initialized()
        
        print(f"✅ Client Type: {agent.client_type}")
        print(f"✅ Model: {agent.model}")
        
        if agent.client_type != 'groq':
             print("❌ WARNING: Not using Groq! Check API Key.")
        
        # Test Generation
        print(f"Testing generation...")
        res = agent._try_generate("Write a 5 word scary CFO story.")
        if res:
             print(f"✅ SUCCESS! Response:\n{res}")
        else:
             print("❌ GENERATION FAILED.")

    except Exception as e:
        print(f"❌ Exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test()
